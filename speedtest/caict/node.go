package caict

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Session carries the per-run identity. The token is NOT stored — it is
// derived per request via ComputeToken (md5 of md5-chains, verified against
// captured traffic). Key starts empty and is filled by IssueSession with the
// node-issued uuid that authorizes download/upload.
type Session struct {
	Key       string // node-issued uuid (empty until IssueSession)
	IMEI      string // device identifier, "TS"+16 uppercase hex
	Model     string // "iphone" (observed iOS traffic) or "Android"
	Pkg       string // applicationId
	Bandwidth int    // nominal bandwidth hint in dovalid
}

// DefaultSession fills a fresh per-run identity (never a captured one).
func DefaultSession(bandwidth int) Session {
	return Session{
		IMEI:      NewDeviceID(),
		Model:     "iphone",
		Pkg:       DefaultPkg,
		Bandwidth: bandwidth,
	}
}

func randHex(n int) string {
	const digits = "0123456789"
	b := make([]byte, n)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = digits[int(b[i])%len(digits)]
	}
	return string(b)
}

const (
	// spoofedUA is the exact UA the engine sends on download requests.
	spoofedUA = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36"
	// dalvikUA is the exact UA the engine sends on upload requests.
	dalvikUA = "Dalvik/1.6.0 (Linux; U; Android 4.2.2; GT-I9505 Build/JDQ39)"
	// uploadBoundary is the hardcoded multipart boundary of the engine.
	uploadBoundary = "00content0boundary00"
	// uploadFormName / uploadFilePrefix mirror the engine's multipart body.
	uploadFormName   = "upload"
	uploadFilePrefix = "SPEED_"
)

var (
	// ErrRejected is returned for a negative dovalid response or HTTP 403.
	ErrRejected        = errors.New("caict: request rejected by node; reason unspecified")
	ErrBusy            = errors.New("caict: node busy (dovalid=0)")
	ErrRetry           = errors.New("caict: node asks retry (dovalid=2)")
	ErrInvalidResponse = errors.New("caict: invalid dovalid issuance response")
)

// DovalidError preserves the exact trimmed response and supports errors.Is.
// Response is kept out of Error() because arbitrary server output may contain
// sensitive data. Callers can inspect it explicitly with errors.As.
type DovalidError struct {
	Response string
	Err      error
}

func (e *DovalidError) Error() string { return e.Err.Error() }
func (e *DovalidError) Unwrap() error { return e.Err }

// DoValid performs the node-side session validation / preheat call.
// start selects flag=true (test begin) or flag=false (test end).
// bandwidth is the nominal test bandwidth in Mbps.
func DoValid(ctx context.Context, srv Server, sess Session, start bool, bandwidth int) (string, error) {
	flag := "false"
	if start {
		flag = "true"
	}
	ts := fmt.Sprintf("%d", time.Now().Unix())
	token := ComputeToken(sess.Model, sess.IMEI, ts, bandwidth)
	q := fmt.Sprintf(
		"http://%s/speed/dovalid?key=%s&flag=%s&bandwidth=%d&model=%s&imei=%s&time=%s&token=%s&pkg=%s",
		srv.Addr(), sess.Key, flag, bandwidth, sess.Model, sess.IMEI, ts, token, sess.Pkg,
	)
	// device traffic shows the IE11 UA on dovalid, not the Chrome spoof
	return httpGetString(ctx, q, ieUA)
}

func httpGetString(ctx context.Context, rawURL, ua string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", errors.New("caict: invalid node request")
	}
	req.Header.Set("User-Agent", ua)
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		// net/http wraps transport errors with the URL, including identity and
		// token query parameters. Preserve the cause without exposing that URL.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return "", fmt.Errorf("caict: node request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("caict: node answered HTTP %d", resp.StatusCode)
	}
	const maxBody = 1 << 16
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return "", fmt.Errorf("caict: read node response: %w", err)
	}
	if len(body) > maxBody {
		return "", errors.New("caict: node response exceeds 64 KiB")
	}
	return string(body), nil
}

// TCPPing measures the TCP handshake RTT to the node (upstream's tcp_ping),
// returning per-probe latencies in milliseconds.
func TCPPing(ctx context.Context, srv Server, count int) ([]float64, error) {
	if count <= 0 {
		count = 4
	}
	rtts := make([]float64, 0, count)
	for i := 0; i < count; i++ {
		start := time.Now()
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", srv.Addr())
		if err != nil {
			return rtts, err
		}
		rtt := float64(time.Since(start).Microseconds()) / 1000.0
		_ = conn.Close()
		rtts = append(rtts, rtt)
	}
	return rtts, nil
}

// DownloadResult aggregates a multi-threaded download test.
type DownloadResult struct {
	Bytes      uint64        // total payload bytes read
	Duration   time.Duration // wall clock of the test window
	AvgBps     float64       // bytes/sec over the window
	MaxBps     float64       // max per-second window across all threads
	Threads    int
	StatusCode int
}

// DownloadTest runs N parallel GETs of /speed/File(1G).dl, mirroring the
// engine's SpeedUpDownTask (multi-connection, sampling, discard body).
// Each connection is closed (Connection: close) after the test window.
func DownloadTest(ctx context.Context, srv Server, sess Session, threads int, duration time.Duration, bandwidthHint int) (*DownloadResult, error) {
	if threads <= 0 {
		threads = 4
	}
	ctx, cancel := context.WithTimeout(ctx, duration+5*time.Second)
	defer cancel()

	res := &DownloadResult{Threads: threads}
	var (
		total  atomic.Uint64
		status atomic.Int32
		sample atomic.Uint64 // bytes since last 1s tick
		wg     sync.WaitGroup
		stop   = make(chan struct{})
	)

	// 1s sampler tracking the max window across all threads.
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				if b := sample.Swap(0); float64(b) > res.MaxBps {
					res.MaxBps = float64(b)
				}
			}
		}
	}()

	// Each worker sends a raw socket GET using the reference client's header
	// set. This avoids transport-added headers; their server-side effects have
	// not been established by a controlled comparison.
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(duration)
			for time.Now().Before(deadline) {
				d := net.Dialer{Timeout: 8 * time.Second}
				conn, err := d.DialContext(ctx, "tcp", srv.Addr())
				if err != nil {
					return
				}
				if tc, ok := conn.(*net.TCPConn); ok {
					_ = tc.SetNoDelay(true)
				}
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				req := fmt.Sprintf(
					"GET /speed/File(1G).dl?r=%d&key=%s HTTP/1.1\r\nAccept: */*\r\nConnection: close\r\nUser-Agent: %s\r\nHost:%s\r\n\r\n",
					time.Now().Unix(), sess.Key, spoofedUA, srv.Addr(),
				)
				if _, err := conn.Write([]byte(req)); err != nil {
					_ = conn.Close()
					return
				}
				statusOK := false
				buf := make([]byte, 64<<10)
				var acc []byte
				for {
					nr, rerr := conn.Read(buf)
					if nr > 0 {
						if !statusOK {
							acc = append(acc, buf[:nr]...)
							if i := strings.Index(string(acc), "\r\n\r\n"); i >= 0 {
								line := strings.SplitN(string(acc[:i]), "\r\n", 2)[0]
								code := 0
								if fs := strings.Fields(line); len(fs) >= 2 {
									_, _ = fmt.Sscanf(fs[1], "%d", &code)
								}
								if code < 200 || code >= 400 {
									status.Store(int32(code))
									_ = conn.Close()
									return
								}
								status.Store(int32(code))
								statusOK = true
								if rest := int64(len(acc) - (i + 4)); rest > 0 {
									total.Add(uint64(rest))
									sample.Add(uint64(rest))
								}
							}
						} else {
							total.Add(uint64(nr))
							sample.Add(uint64(nr))
						}
					}
					if rerr != nil {
						break
					}
					if time.Now().After(deadline) {
						break
					}
				}
				_ = conn.Close()
			}
		}()
	}

	// Stop the workers at the end of the window.
	done := make(chan struct{})
	go func() {
		<-time.After(duration)
		close(stop)
		done <- struct{}{}
	}()
	<-done
	cancel() // unblock in-flight reads

	wg.Wait()
	res.Bytes = total.Load()
	res.Duration = duration
	res.AvgBps = float64(res.Bytes) / duration.Seconds()
	res.StatusCode = int(status.Load())
	if res.Bytes == 0 {
		if res.StatusCode == http.StatusForbidden {
			return res, ErrRejected
		}
		if res.StatusCode != 0 && res.StatusCode != http.StatusOK {
			return res, fmt.Errorf("caict: node answered HTTP %d", res.StatusCode)
		}
		return res, errors.New("caict: no data received (rejected or unreachable)")
	}
	return res, nil
}

// UploadResult aggregates a multipart upload test.
type UploadResult struct {
	Bytes      uint64
	Duration   time.Duration
	AvgBps     float64
	MaxBps     float64
	Threads    int
	StatusCode int
}

// UploadTest POSTs random data as multipart/form-data with the engine's
// hardcoded boundary and "Key:" session header, mirroring its upload path.
func UploadTest(ctx context.Context, srv Server, sess Session, threads int, duration time.Duration, chunk int) (*UploadResult, error) {
	if threads <= 0 {
		threads = 2
	}
	if chunk <= 0 {
		chunk = 256 << 10
	}
	q := fmt.Sprintf("http://%s/speed/doAnalsLoad.do", srv.Addr())
	ctx, cancel := context.WithTimeout(ctx, duration+5*time.Second)
	defer cancel()

	res := &UploadResult{Threads: threads}
	var (
		total   atomic.Uint64
		status  atomic.Int32
		sample  atomic.Uint64
		lastErr atomic.Pointer[error]
		wg      sync.WaitGroup
		stop    = make(chan struct{})
	)
	recordErr := func(err error) { lastErr.Store(&err) }

	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				if b := sample.Swap(0); float64(b) > res.MaxBps {
					res.MaxBps = float64(b)
				}
			}
		}
	}()

	payload := make([]byte, chunk)
	_, _ = rand.Read(payload)

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(duration)
			for time.Now().Before(deadline) {
				pr, pw := io.Pipe()
				mw := multipart.NewWriter(pw)
				_ = mw.SetBoundary(uploadBoundary)
				go func() {
					defer pw.CloseWithError(io.EOF)
					part, err := mw.CreatePart(textproto.MIMEHeader{
						"Content-Disposition": {fmt.Sprintf(
							`form-data; name=%q;filename=%q`, uploadFormName, uploadFilePrefix+randHex(12))},
						"Content-Type": {"application/octet-stream"},
					})
					if err != nil {
						_ = pw.CloseWithError(err)
						return
					}
					for time.Now().Before(deadline) {
						if _, err := part.Write(payload); err != nil {
							_ = pw.CloseWithError(err)
							return
						}
						total.Add(uint64(len(payload)))
						sample.Add(uint64(len(payload)))
					}
					_ = mw.Close() // writes the terminating boundary
				}()

				req, err := http.NewRequestWithContext(ctx, http.MethodPost, q, pr)
				if err != nil {
					recordErr(err)
					return
				}
				req.Header.Set("User-Agent", dalvikUA)
				req.Header.Set("Content-Type", "multipart/form-data;boundary="+uploadBoundary)
				req.Header.Set("Cache-Control", "no-cache")
				req.Header.Set("Charset", "UTF-8")
				req.Header.Set("Key", sess.Key)
				req.ContentLength = -1 // chunked; the node streams until close
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					if ctx.Err() == nil {
						recordErr(err)
					}
					return
				}
				status.Store(int32(resp.StatusCode))
				_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
				_ = resp.Body.Close()
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		<-time.After(duration)
		close(stop)
		done <- struct{}{}
	}()
	<-done
	cancel()

	wg.Wait()
	res.Bytes = total.Load()
	res.Duration = duration
	res.AvgBps = float64(res.Bytes) / duration.Seconds()
	res.StatusCode = int(status.Load())
	if res.StatusCode == 0 {
		if err := lastErr.Load(); err != nil {
			return res, *err
		}
	}
	if res.Bytes == 0 {
		if res.StatusCode == http.StatusForbidden {
			return res, ErrRejected
		}
		return res, errors.New("caict: upload rejected or unreachable")
	}
	return res, nil
}

// reportLine renders the engine's speedtest CSV schema:
// event_time;server_id;server_name;ping_delay;ping_loss;rtt_jitter;tcp_shake_delay;down_avg;down_max;up_avg;up_max;
func reportLine(now time.Time, srv Server, pingAvgMs, loss, jitterMs, downAvg, downMax, upAvg, upMax float64) string {
	const layout = "2006-01-02 15:04:05"
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }
	return strings.Join([]string{
		now.Format(layout), srv.HostID, srv.HostName,
		f(pingAvgMs), f(loss), f(jitterMs), "0",
		f(downAvg / (1 << 20)), f(downMax / (1 << 20)), f(upAvg / (1 << 20)), f(upMax / (1 << 20)),
		"",
	}, ";")
}

// ReportLine is the exported CSV row builder (upload speeds in Mbps).
func ReportLine(srv Server, pingAvgMs, loss, jitterMs, downAvg, downMax, upAvg, upMax float64) string {
	return reportLine(time.Now(), srv, pingAvgMs, loss, jitterMs, downAvg, downMax, upAvg, upMax)
}

// pingStats reduces per-probe RTTs into the values the report needs.
type pingStats struct {
	Avg, Max, Jitter float64
	Loss             float64
	Count            int
}

func pingSummary(rtts []float64, errs int) pingStats {
	if len(rtts) == 0 {
		return pingStats{Loss: 100}
	}
	sum, max := 0.0, 0.0
	for _, r := range rtts {
		sum += r
		if r > max {
			max = r
		}
	}
	avg := sum / float64(len(rtts))
	// jitter: mean absolute delta of successive probes (RFC 3550 style)
	var jsum float64
	for i := 1; i < len(rtts); i++ {
		d := rtts[i] - rtts[i-1]
		if d < 0 {
			d = -d
		}
		jsum += d
	}
	jitter := 0.0
	if len(rtts) > 1 {
		jitter = jsum / float64(len(rtts)-1)
	}
	total := len(rtts) + errs
	return pingStats{
		Avg:    avg,
		Max:    max,
		Jitter: jitter,
		Loss:   float64(errs) / float64(total) * 100,
		Count:  len(rtts),
	}
}
