package caict

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Control-plane protocol, verified against device traffic (HAR, 2026-11) and
// cross-checked with the reference implementation of the same protocol
// (github.com/MiaM1ku/taierspeedtest, RE'd from v4.4.8):
//
//	GET {base}/dataServer/getIpLocSP.php
//	  → ip|[国家,省,市,区,运营商]|operId|operName|cityCode
//
//	GET {base}/dataServer/mobilematch_many.php
//	    ?ip=&network=&province=&city=&wifioper=&mobileoperid=&ipv6=&model=&pkg=
//	  → JSON array of {hostid,hostname,hostip,port,pname,city,...}
//
//	GET http://{node}/speed/dovalid?key=&flag=true&bandwidth=&model=&imei=
//	    &time={unix}&token=&pkg=      (Android adds &app=globalspeed)
//	  → "1-{uuid}" session issuance; "0" busy; "2" retry; "-1" invalid
//
// token (verified against a captured iOS request):
//
//	token = md5( md5("model="+model+"&imei="+id)
//	           + md5("stime="+unix+"&band="+band+"&rand=12345555") )
//
// Session end: POST /speed/dovalid?key={uuid}. Uploads go to
// /speed/doAnalsLoad.do with the multipart body described in node.go.
const (
	tokenSalt = "12345555"

	// ieUA is the UA the app sends on control-plane and dovalid requests
	// (observed in device traffic; the Chrome/Dalvik spoofs are node-side).
	ieUA = "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko"

	DefaultControlBase   = "https://dlcv2.cnspeedtest.cn:8443"
	DefaultControlBaseV6 = "https://dlcv64.cnspeedtest.cn:8443"
	DefaultPkg           = "com.cnspeedtest.globalspeed"
	appNameParam         = "globalspeed"
)

// ctrlBases is the control-plane failover chain (dlcv2 → vendor mirrors).
var ctrlBases = []string{
	DefaultControlBase,
	"https://dlc.duoweisoft.com:8096",
	"http://dlcv2.duoweisoft.com:8088",
}

// NewDeviceID generates a device identifier in the app's format:
// "TS" + 16 uppercase hex chars (8 random bytes).
func NewDeviceID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "TS" + strings.ToUpper(hex.EncodeToString(b))
}

// md5hex is the MD5 hex digest helper used by the token formula.
func md5hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// ComputeToken derives the dovalid request token. The formula was checked
// offline against captured traffic; tests use synthetic identities.
func ComputeToken(model, deviceID, unixTS string, bandwidth int) string {
	h1 := md5hex("model=" + model + "&imei=" + deviceID)
	h2 := md5hex(fmt.Sprintf("stime=%s&band=%d&rand=%s", unixTS, bandwidth, tokenSalt))
	return md5hex(h1 + h2)
}

// ControlPlane talks to the PHP control plane (dataServer endpoints).
type ControlPlane struct {
	Base  string // e.g. https://dlcv2.cnspeedtest.cn:8443
	Pkg   string
	Model string // "iphone" (iOS traffic) / "Android"
	DevID string // TS-prefixed device identifier
	HTTP  *http.Client
}

// NewControlPlane fills sane defaults with a fresh device identity.
func NewControlPlane() *ControlPlane {
	return &ControlPlane{
		Base:  DefaultControlBase,
		Pkg:   DefaultPkg,
		Model: "iphone",
		DevID: NewDeviceID(),
		HTTP:  &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *ControlPlane) get(ctx context.Context, rawurl string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawurl, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", ieUA)
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Accept", "*/*")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return string(body), nil
}

// IpLoc is the parsed getIpLocSP.php pipe-delimited response.
type IpLoc struct {
	OuterIP  string
	Region   []string // [国家, 省, 市, 区, 运营商]
	OperID   string
	OperName string
	CityCode string
}

// GetIpLoc resolves the caller's public IP / region / ISP, failing over
// across the control-plane mirrors.
func (c *ControlPlane) GetIpLoc(ctx context.Context) (*IpLoc, error) {
	bases := []string{c.Base}
	for _, b := range ctrlBases {
		if b != c.Base {
			bases = append(bases, b)
		}
	}
	var lastErr error
	for _, base := range bases {
		body, err := c.get(ctx, base+"/dataServer/getIpLocSP.php?tag=ios&mac="+url.QueryEscape(c.DevID))
		if err != nil {
			lastErr = err
			continue
		}
		loc, perr := parseIpLoc(body)
		if perr != nil {
			lastErr = perr
			continue
		}
		c.Base = base // stick with the working mirror
		return loc, nil
	}
	return nil, lastErr
}

func parseIpLoc(body string) (*IpLoc, error) {
	parts := strings.Split(strings.TrimSpace(body), "|")
	if len(parts) < 4 || parts[0] == "" {
		return nil, fmt.Errorf("caict: unexpected getIpLocSP response %q", truncate(body, 120))
	}
	loc := &IpLoc{OuterIP: parts[0], OperID: parts[2], OperName: parts[3]}
	if len(parts) >= 5 {
		loc.CityCode = parts[4]
	}
	if err := json.Unmarshal([]byte(parts[1]), &loc.Region); err != nil {
		loc.Region = []string{parts[1]}
	}
	return loc, nil
}

// MatchServers asks the control plane for live nodes matching the caller's
// location/ISP. This is the same feed the app uses; the embedded decrypted
// list is the offline fallback.
func (c *ControlPlane) MatchServers(ctx context.Context, loc *IpLoc, ipv6 bool) ([]Server, error) {
	province, city := "", ""
	if loc != nil && len(loc.Region) >= 3 {
		province, city = loc.Region[1], loc.Region[2]
	}
	oper, outerIP := "", ""
	if loc != nil {
		oper, outerIP = loc.OperName, loc.OuterIP
	}
	v := url.Values{}
	v.Set("ip", outerIP)
	v.Set("network", "4")
	v.Set("province", province)
	v.Set("city", city)
	v.Set("wifioper", oper)
	v.Set("mobileoperid", "")
	if ipv6 {
		v.Set("ipv6", "1")
	} else {
		v.Set("ipv6", "0")
	}
	v.Set("model", c.Model)
	v.Set("pkg", c.Pkg)
	body, err := c.get(ctx, c.Base+"/dataServer/mobilematch_many.php?"+v.Encode())
	if err != nil {
		return nil, err
	}
	var arr []map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(body)), &arr); err != nil {
		return nil, fmt.Errorf("caict: mobilematch response %q: %w", truncate(body, 120), err)
	}
	out := make([]Server, 0, len(arr))
	for _, item := range arr {
		port := ""
		switch p := item["port"].(type) {
		case string:
			port = p
		case float64:
			port = fmt.Sprintf("%d", int(p))
		}
		oper := oper
		if o, ok := item["oper"].(string); ok && o != "" {
			oper = o
		}
		out = append(out, Server{
			HostID:   fmt.Sprint(item["hostid"]),
			HostName: fmt.Sprint(item["hostname"]),
			HostIP:   fmt.Sprint(item["hostip"]),
			Port:     port,
			PName:    fmt.Sprint(item["pname"]),
			Location: fmt.Sprint(item["city"]),
			Oper:     oper,
		})
	}
	return out, nil
}

var issuedSessionPattern = regexp.MustCompile(`^1-([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$`)

// issueResponse accepts only a complete 1-{canonical UUID} issuance answer.
func issueResponse(body string) (string, error) {
	s := strings.TrimSpace(body)
	if issuedSessionPattern.MatchString(s) {
		return s[2:], nil
	}
	cause := ErrInvalidResponse
	switch s {
	case "0", "0-":
		cause = ErrBusy
	case "2", "2-":
		cause = ErrRetry
	case "-1", "-1-":
		cause = ErrRejected
	}
	return "", &DovalidError{Response: s, Err: cause}
}

// IssueSession performs the key-issuance handshake: dovalid with an EMPTY
// key and a computed token; the node answers "1-{uuid}" and that UUID
// authorizes the subsequent download/upload calls. The enqueue timestamp is
// returned as well: the app reuses it as the r= parameter of the download
// requests.
func IssueSession(ctx context.Context, srv Server, ident Session) (string, string, error) {
	ts := fmt.Sprintf("%d", time.Now().Unix())
	token := ComputeToken(ident.Model, ident.IMEI, ts, ident.Bandwidth)
	q := fmt.Sprintf(
		"http://%s/speed/dovalid?key=&flag=true&bandwidth=%d&model=%s&imei=%s&time=%s&token=%s&pkg=%s",
		srv.Addr(), ident.Bandwidth, ident.Model, ident.IMEI, ts, token, ident.Pkg,
	)
	if ident.Model == "Android" {
		q += "&app=" + appNameParam
	}
	body, err := httpGetString(ctx, q, ieUA)
	if err != nil {
		return "", "", err
	}
	uuid, err := issueResponse(body)
	return uuid, ts, err
}

// Dequeue ends the test session (POST /speed/dovalid?key={uuid}).
func Dequeue(ctx context.Context, srv Server, key string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+srv.Addr()+"/speed/dovalid?key="+url.QueryEscape(key), strings.NewReader(""))
	if err == nil {
		req.Header.Set("User-Agent", ieUA)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
		}
	}
}

// DownloadProbe requires HTTP 200/206 and n payload bytes from the download
// stream. It retains the existing request headers and timestamp parameter;
// this probe does not establish which request fields a production node requires.
func DownloadProbe(ctx context.Context, srv Server, key string, n int64) (total int64, status int, err error) {
	if n <= 0 {
		return 0, 0, errors.New("caict: probe size must be positive")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	defer func() {
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			err = ctx.Err()
			return
		}
		// The socket deadline and context timer can fire in either order.
		// Preserve context semantics even if the socket reports its timeout first.
		var timeout net.Error
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) && errors.As(err, &timeout) && timeout.Timeout() {
			err = context.DeadlineExceeded
		}
	}()
	addr := srv.Addr()
	d := net.Dialer{Timeout: 8 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return 0, 0, err
	}
	req := fmt.Sprintf(
		"GET /speed/File(1G).dl?r=%d&key=%s HTTP/1.1\r\nAccept: */*\r\nConnection: close\r\nUser-Agent: %s\r\nHost:%s\r\n\r\n",
		time.Now().Unix(), url.QueryEscape(key), spoofedUA, addr,
	)
	if _, err := io.WriteString(conn, req); err != nil {
		return 0, 0, err
	}
	// Bound the parser's input while reading headers, then lift the bound
	// for the body. The buffered reader retains any prefetched payload.
	headerInput := &io.LimitedReader{R: conn, N: 64 << 10}
	resp, err := http.ReadResponse(bufio.NewReader(headerInput), nil)
	if err != nil {
		return 0, 0, fmt.Errorf("caict: read probe response: %w", err)
	}
	defer func() {
		// Closing the socket first prevents Body.Close from draining a large
		// response from a peer that ignores Connection: close.
		_ = conn.Close()
		_ = resp.Body.Close()
	}()
	headerInput.N = 1<<63 - 1
	status = resp.StatusCode
	if status == http.StatusForbidden {
		return 0, status, ErrRejected
	}
	if status != http.StatusOK && status != http.StatusPartialContent {
		return 0, status, fmt.Errorf("caict: probe answered HTTP %d", status)
	}
	body := io.LimitReader(resp.Body, n)
	buf := make([]byte, 32<<10)
	for total < n {
		want := min(int64(len(buf)), n-total)
		got, err := io.ReadFull(body, buf[:want])
		total += int64(got)
		if err != nil {
			return total, status, fmt.Errorf("caict: probe read %d of %d bytes: %w", total, n, err)
		}
	}
	return total, status, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
