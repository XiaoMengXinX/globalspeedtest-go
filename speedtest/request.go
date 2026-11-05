package speedtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XiaoMengXinX/globalspeedtest-go/speedtest/caict"
)

type (
	downloadFunc func(context.Context, *Server, int) error
	uploadFunc   func(context.Context, *Server, int) error
)

var ulSizes = [...]int{100, 300, 500, 800, 1000, 1500, 2500, 3000, 3500, 4000}

var ErrConnectTimeout = errors.New("server connect timeout")

const (
	nodeControlUA  = "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko"
	nodeDownloadUA = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36"
	nodeUploadUA   = "Dalvik/1.6.0 (Linux; U; Android 4.2.2; GT-I9505 Build/JDQ39)"
	uploadBoundary = "00content0boundary00"
)

type nodeSession struct {
	key       string
	timestamp string
}

type nodeSessionContextKey struct{}

var nodeSessionPattern = regexp.MustCompile(`^1-([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$`)

type queryParam struct{ key, value string }

func (s *Server) nodeURL(path string, params []queryParam) (string, error) {
	u, err := url.Parse(s.URL)
	if err != nil {
		return "", err
	}
	if u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", errors.New("invalid node URL")
	}
	var raw strings.Builder
	for i, p := range params {
		if i > 0 {
			raw.WriteByte('&')
		}
		raw.WriteString(url.QueryEscape(p.key))
		raw.WriteByte('=')
		raw.WriteString(url.QueryEscape(p.value))
	}
	u.Path, u.RawPath, u.RawQuery, u.Fragment = path, path, raw.String(), ""
	return u.String(), nil
}

func nodeRequestError(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return urlErr.Err
	}
	return err
}

func (s *Server) issueSession(ctx context.Context) (nodeSession, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	config := s.Context.config
	query := []queryParam{
		{"key", ""}, {"flag", "true"}, {"bandwidth", strconv.Itoa(config.Bandwidth)},
		{"model", "iphone"}, {"imei", config.DeviceID}, {"time", stamp},
		{"token", caict.ComputeToken("iphone", config.DeviceID, stamp, config.Bandwidth)},
		{"pkg", caict.DefaultPkg},
	}
	rawURL, err := s.nodeURL("/speed/dovalid", query)
	if err != nil {
		return nodeSession{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nodeSession{}, err
	}
	req.Header.Set("User-Agent", nodeControlUA)
	resp, err := s.Context.doer.Do(req)
	if err != nil {
		return nodeSession{}, nodeRequestError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nodeSession{}, fmt.Errorf("session request: HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil {
		return nodeSession{}, err
	}
	answer := strings.TrimSpace(string(body))
	match := nodeSessionPattern.FindStringSubmatch(answer)
	if len(body) <= 4096 && match != nil {
		return nodeSession{key: match[1], timestamp: stamp}, nil
	}
	cause := caict.ErrInvalidResponse
	switch answer {
	case "0", "0-":
		cause = caict.ErrBusy
	case "2", "2-":
		cause = caict.ErrRetry
	case "-1", "-1-":
		cause = caict.ErrRejected
	}
	return nodeSession{}, &caict.DovalidError{Response: answer, Err: cause}
}

func (s *Server) releaseSession(session nodeSession) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rawURL, err := s.nodeURL("/speed/dovalid", []queryParam{{"key", session.key}})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", nodeControlUA)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := s.Context.doer.Do(req)
	if err != nil {
		return nodeRequestError(err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, 4096)); err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("release session: HTTP %d", resp.StatusCode)
	}
	return nil
}

func (s *Server) MultiDownloadTestContext(ctx context.Context, servers Servers) error {
	return s.measureContext(ctx, servers, false, downloadRequest)
}

func (s *Server) MultiUploadTestContext(ctx context.Context, servers Servers) error {
	return s.measureContext(ctx, servers, true, uploadRequest)
}

// DownloadTest measures download speed.
func (s *Server) DownloadTest() error {
	return s.DownloadTestContext(context.Background())
}

// DownloadTestContext measures download speed until the capture window ends or ctx is canceled.
func (s *Server) DownloadTestContext(ctx context.Context) error {
	return s.downloadTestContext(ctx, s.downloadRequest())
}

func (s *Server) downloadTestContext(ctx context.Context, request downloadFunc) error {
	return s.measureContext(ctx, Servers{s}, false, request)
}

// UploadTest measures upload speed.
func (s *Server) UploadTest() error {
	return s.UploadTestContext(context.Background())
}

// UploadTestContext measures confirmed upload speed.
func (s *Server) UploadTestContext(ctx context.Context) error {
	return s.uploadTestContext(ctx, s.uploadRequest())
}

func (s *Server) downloadRequest() downloadFunc { return downloadRequest }
func (s *Server) uploadRequest() uploadFunc     { return uploadRequest }

func (s *Server) uploadTestContext(ctx context.Context, request uploadFunc) error {
	return s.measureContext(ctx, Servers{s}, true, request)
}

func (s *Server) measureContext(ctx context.Context, servers Servers, upload bool, request func(context.Context, *Server, int) error) (result error) {
	if s.Context == nil || s.Context.Manager == nil {
		return ErrorUninitializedManager
	}
	manager := s.Context.Manager
	if upload {
		manager.PrepareUpload()
		s.ULSpeed = 0
	} else {
		manager.PrepareDownload()
		s.DLSpeed = 0
	}
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		if upload {
			s.ULSpeed = ByteRate(manager.GetEWMAUploadRate())
			s.TestDuration.Upload = &duration
			if result != nil && manager.GetTotalUpload() == 0 {
				s.ULSpeed = -1
			}
		} else {
			s.DLSpeed = ByteRate(manager.GetEWMADownloadRate())
			s.TestDuration.Download = &duration
			if result != nil && manager.GetTotalDownload() == 0 {
				s.DLSpeed = -1
			}
		}
		s.testDurationTotalCount()
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	available := servers.Available()
	if available.Len() == 0 {
		return errors.New("not found available servers")
	}
	participants := append(Servers{}, *available...)
	for i, server := range participants {
		if server == s || (s.ID != "" && server.ID == s.ID) {
			participants[0], participants[i] = participants[i], participants[0]
			break
		}
	}
	if limit := manager.WorkerLimit(upload); len(participants) > limit {
		participants = participants[:limit]
	}
	phaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var td *TestDirection
	var workerErr error
	var firstError sync.Once
	for _, server := range participants {
		if server.Context == nil {
			return ErrorUninitializedManager
		}
		session, err := server.issueSession(phaseCtx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		defer func() { result = errors.Join(result, server.releaseSession(session)) }()
		workerContext := Speedtest{
			Manager:   manager,
			doer:      server.Context.doer,
			config:    server.Context.config,
			tcpDialer: server.Context.tcpDialer,
			udpDialer: server.Context.udpDialer,
			ipDialer:  server.Context.ipDialer,
		}
		workerServer := *server
		workerServer.Context = &workerContext
		requestCtx := context.WithValue(phaseCtx, nodeSessionContextKey{}, session)
		worker := func() {
			if phaseCtx.Err() != nil {
				return
			}
			if err := request(requestCtx, &workerServer, 4); err != nil && phaseCtx.Err() == nil {
				firstError.Do(func() { workerErr = err; cancel() })
			}
		}
		if upload {
			td = manager.RegisterUploadHandler(worker)
		} else {
			td = manager.RegisterDownloadHandler(worker)
		}
	}
	if td == nil {
		return ErrorUninitializedManager
	}
	td.StartContext(phaseCtx, cancel, 0)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return workerErr
}

func downloadRequest(ctx context.Context, s *Server, _ int) error {
	session, ok := ctx.Value(nodeSessionContextKey{}).(nodeSession)
	if !ok {
		return errors.New("download session is missing")
	}
	rawURL, err := s.nodeURL("/speed/File(1G).dl", []queryParam{
		{"r", session.timestamp}, {"key", session.key},
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", nodeDownloadUA)
	req.Header.Set("Accept", "*/*")
	req.Close = true
	resp, err := s.Context.doer.Do(req)
	if err != nil {
		return nodeRequestError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("download request: HTTP %d", resp.StatusCode)
	}
	return s.Context.NewChunk().DownloadHandler(resp.Body)
}

type uploadBody struct {
	io.Reader
	read atomic.Int64
}

func (b *uploadBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read.Add(int64(n))
	return n, err
}

func uploadRequest(ctx context.Context, s *Server, w int) error {
	session, ok := ctx.Value(nodeSessionContextKey{}).(nodeSession)
	if !ok {
		return errors.New("upload session is missing")
	}
	if w < 0 || w >= len(ulSizes) {
		return errors.New("invalid upload size")
	}
	size := int64(ulSizes[w]*100-51) * 10
	var framing bytes.Buffer
	form := multipart.NewWriter(&framing)
	if err := form.SetBoundary(uploadBoundary); err != nil {
		return err
	}
	header := textproto.MIMEHeader{}
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="upload"; filename="SPEED_%d"`, time.Now().UnixNano()))
	header.Set("Content-Type", "application/octet-stream")
	if _, err := form.CreatePart(header); err != nil {
		return err
	}
	prefixLength := framing.Len()
	if err := form.Close(); err != nil {
		return err
	}
	body := &uploadBody{Reader: io.MultiReader(bytes.NewReader(framing.Bytes()[:prefixLength]), s.Context.NewChunk().UploadHandler(size), bytes.NewReader(framing.Bytes()[prefixLength:]))}
	rawURL, err := s.nodeURL("/speed/doAnalsLoad.do", nil)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, io.NopCloser(body))
	if err != nil {
		return err
	}
	req.ContentLength = size + int64(framing.Len())
	req.Header.Set("Content-Type", form.FormDataContentType())
	req.Header.Set("Key", session.key)
	req.Header.Set("User-Agent", nodeUploadUA)
	req.Header.Set("Accept", "*/*")
	req.Close = true
	resp, err := s.Context.doer.Do(req)
	if err != nil {
		return nodeRequestError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("upload request: HTTP %d", resp.StatusCode)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return err
	}
	if body.read.Load() != req.ContentLength {
		return io.ErrUnexpectedEOF
	}
	s.Context.AddTotalUpload(size)
	return nil
}

// TCPTest uses the node's HTTP service for throughput.
func tcpDownloadRequest(ctx context.Context, s *Server, w int) error {
	return downloadRequest(ctx, s, w)
}
func tcpUploadRequest(ctx context.Context, s *Server, w int) error { return uploadRequest(ctx, s, w) }

func tcpHost(s *Server) (string, error) {
	u, err := url.Parse(s.URL)
	if err != nil {
		return "", err
	}
	host := s.Host
	if host == "" {
		host = u.Host
	}
	if host == "" {
		return "", errors.New("tcp server host is empty")
	}
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host, nil
	}
	port := "80"
	if u.Scheme == "https" {
		port = "443"
	}
	return net.JoinHostPort(strings.Trim(host, "[]"), port), nil
}

// PingTest measures latency.
func (s *Server) PingTest(callback func(latency time.Duration)) error {
	return s.PingTestContext(context.Background(), callback)
}

// PingTestContext measures latency and jitter, observing ctx.
func (s *Server) PingTestContext(ctx context.Context, callback func(latency time.Duration)) (err error) {
	start := time.Now()
	var samples []int64
	var summary probeSummary
	ctx = context.WithValue(ctx, probeSummaryContextKey{}, &summary)
	switch s.Context.config.PingMode {
	case TCP:
		samples, err = s.TCPPing(ctx, 10, 200*time.Millisecond, callback)
	case ICMP:
		samples, err = s.ICMPPing(ctx, 4*time.Second, 10, 200*time.Millisecond, callback)
	default:
		samples, err = s.HTTPPing(ctx, 10, 200*time.Millisecond, callback)
	}
	s.PacketLoss.Sent, s.PacketLoss.Dup, s.PacketLoss.Max = summary.successes, 0, summary.attempts-1
	if len(samples) > 0 {
		mean, _, std, min, max := StandardDeviation(samples)
		s.Latency, s.Jitter = time.Duration(mean), time.Duration(std)
		s.MinLatency, s.MaxLatency = time.Duration(min), time.Duration(max)
	} else {
		s.Latency = PingTimeout
	}
	duration := time.Since(start)
	s.TestDuration.Ping = &duration
	s.testDurationTotalCount()
	return err
}

// TestAll measures ping, download, and upload in sequence.
func (s *Server) TestAll() error {
	if err := s.PingTest(nil); err != nil {
		return err
	}
	if err := s.DownloadTest(); err != nil {
		return err
	}
	return s.UploadTest()
}

func waitProbe(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type probeSummary struct{ attempts, successes int }
type probeSummaryContextKey struct{}

func (s *Server) probe(ctx context.Context, count int, interval time.Duration, callback func(time.Duration), request func(context.Context) error) (latencies []int64, err error) {
	var summary probeSummary
	defer func() {
		if result, ok := ctx.Value(probeSummaryContextKey{}).(*probeSummary); ok {
			*result = summary
		}
	}()
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return latencies, err
		}
		summary.attempts++
		start := time.Now()
		probeCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
		err := request(probeCtx)
		cancel()
		elapsed := time.Since(start)
		if ctx.Err() != nil {
			return latencies, ctx.Err()
		}
		if err == nil {
			summary.successes++
			latencies = append(latencies, elapsed.Nanoseconds())
			if callback != nil {
				callback(elapsed)
			}
		}
		if i+1 < count {
			if err := waitProbe(ctx, interval); err != nil {
				return latencies, err
			}
		}
	}
	if len(latencies) == 0 {
		return nil, ErrConnectTimeout
	}
	return latencies, nil
}

// TCPPing measures TCP handshake latency using the configured dialer.
func (s *Server) TCPPing(ctx context.Context, echoTimes int, echoFreq time.Duration, callback func(time.Duration)) ([]int64, error) {
	host, err := tcpHost(s)
	if err != nil {
		return nil, err
	}
	return s.probe(ctx, echoTimes, echoFreq, callback, func(ctx context.Context) error {
		conn, err := s.Context.tcpDialer.DialContext(ctx, "tcp", host)
		if err != nil {
			return err
		}
		return conn.Close()
	})
}

// HTTPPing accepts node-root responses below HTTP 500, including HTTP 404.
func (s *Server) HTTPPing(ctx context.Context, echoTimes int, echoFreq time.Duration, callback func(time.Duration)) ([]int64, error) {
	rawURL, err := s.nodeURL("/", nil)
	if err != nil {
		return nil, err
	}
	return s.probe(ctx, echoTimes, echoFreq, callback, func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return err
		}
		resp, err := s.Context.doer.Do(req)
		if err != nil {
			return nodeRequestError(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 500 {
			return fmt.Errorf("ping request: HTTP %d", resp.StatusCode)
		}
		return nil
	})
}

const PingTimeout = -1
const echoOptionDataSize = 32

// ICMPPing requires permission to open an ICMP socket.
func (s *Server) ICMPPing(ctx context.Context, readTimeout time.Duration, echoTimes int, echoFreq time.Duration, callback func(time.Duration)) (latencies []int64, err error) {
	u, err := url.Parse(s.URL)
	if err != nil {
		return nil, err
	}
	if u.Hostname() == "" {
		return nil, errors.New("icmp server host is empty")
	}
	conn, err := s.Context.ipDialer.DialContext(ctx, "ip:icmp", u.Hostname())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	packet := make([]byte, 8+echoOptionDataSize)
	packet[0], packet[5] = 8, 1
	copy(packet[8:], "Speedtest")
	return s.probe(ctx, echoTimes, echoFreq, callback, func(probeCtx context.Context) error {
		packet[2], packet[3] = 0, 0
		packet[7]++
		checksum := checkSum(packet)
		packet[2], packet[3] = byte(checksum>>8), byte(checksum)
		deadline := time.Now().Add(readTimeout)
		if d, ok := probeCtx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		if err := conn.SetDeadline(deadline); err != nil {
			return err
		}
		if _, err := conn.Write(packet); err != nil {
			return err
		}
		var reply [512]byte
		n, err := conn.Read(reply[:])
		if err != nil {
			return err
		}
		offset := 0
		if n > 0 && reply[0]>>4 == 4 {
			offset = int(reply[0]&15) * 4
		}
		if n < offset+8 || reply[offset] != 0 || reply[offset+5] != packet[5] || reply[offset+7] != packet[7] {
			return errors.New("invalid ICMP echo reply")
		}
		return nil
	})
}

func checkSum(data []byte) uint16 {
	var sum uint32
	for len(data) > 1 {
		sum += uint32(data[0])<<8 | uint32(data[1])
		data = data[2:]
	}
	if len(data) > 0 {
		sum += uint32(data[0]) << 8
	}
	for sum>>16 != 0 {
		sum = sum&0xffff + sum>>16
	}
	return ^uint16(sum)
}

func StandardDeviation(vector []int64) (mean, variance, stdDev, min, max int64) {
	if len(vector) == 0 {
		return
	}
	var sum, accumulate int64
	min, max = math.MaxInt64, math.MinInt64
	for _, value := range vector {
		sum += value
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	mean = sum / int64(len(vector))
	for _, value := range vector {
		accumulate += (value - mean) * (value - mean)
	}
	variance = accumulate / int64(len(vector))
	stdDev = int64(math.Sqrt(float64(variance)))
	return
}
