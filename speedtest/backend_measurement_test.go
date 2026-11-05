package speedtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XiaoMengXinX/globalspeedtest-go/speedtest/caict"
)

const measurementDeviceID = "TS0123456789ABCDEF"

type measurementNode struct {
	server           *httptest.Server
	mu               sync.Mutex
	issues, releases int
	keys             map[string]bool
	uploaded         atomic.Int64
	downloads        atomic.Int64
	hook             func(http.ResponseWriter, *http.Request) bool
}

func newMeasurementFixture(t *testing.T) (*Speedtest, *Server, *measurementNode) {
	t.Helper()
	node := &measurementNode{keys: make(map[string]bool)}
	node.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if node.hook != nil && node.hook(w, r) {
			return
		}
		switch r.URL.Path {
		case "/speed/dovalid":
			switch r.Method {
			case http.MethodGet:
				q := r.URL.Query()
				bandwidth, _ := strconv.Atoi(q.Get("bandwidth"))
				if q.Get("imei") != measurementDeviceID || q.Get("key") != "" || !q.Has("key") || q.Get("flag") != "true" || q.Get("model") != "iphone" || q.Get("pkg") != caict.DefaultPkg || bandwidth != 500 || q.Get("token") != caict.ComputeToken("iphone", measurementDeviceID, q.Get("time"), bandwidth) {
					t.Errorf("invalid session parameters")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if r.UserAgent() != nodeControlUA {
					t.Errorf("session UA=%q", r.UserAgent())
				}
				node.mu.Lock()
				node.issues++
				key := fmt.Sprintf("00000000-0000-4000-8000-%012d", node.issues)
				node.keys[key] = true
				node.mu.Unlock()
				_, _ = fmt.Fprint(w, "1-"+key)
			case http.MethodPost:
				if r.Context().Err() != nil {
					t.Error("release inherited canceled context")
				}
				node.mu.Lock()
				key := r.URL.Query().Get("key")
				if !node.keys[key] {
					t.Error("release for inactive session")
				}
				delete(node.keys, key)
				node.releases++
				node.mu.Unlock()
				_, _ = fmt.Fprint(w, "0")
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		case "/speed/File(1G).dl":
			if !node.active(r.URL.Query().Get("key")) {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			if r.Method != http.MethodGet || r.UserAgent() != nodeDownloadUA || r.URL.Query().Has("old") {
				t.Error("invalid download request")
			}
			if stamp, err := strconv.ParseInt(r.URL.Query().Get("r"), 10, 64); err != nil || stamp <= 0 {
				t.Error("invalid download timestamp")
			}
			node.downloads.Add(1)
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(bytes.Repeat([]byte("x"), 512))
			w.(http.Flusher).Flush()
			_ = waitProbe(r.Context(), time.Millisecond)
		case "/speed/doAnalsLoad.do":
			if !node.active(r.Header.Get("Key")) {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			if r.Method != http.MethodPost || r.UserAgent() != nodeUploadUA || r.ContentLength <= 0 || len(r.TransferEncoding) != 0 {
				t.Error("invalid upload request")
			}
			mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil || mediaType != "multipart/form-data" || params["boundary"] != uploadBoundary {
				t.Error("invalid upload multipart header")
				w.WriteHeader(400)
				return
			}
			reader := multipart.NewReader(r.Body, params["boundary"])
			part, err := reader.NextPart()
			if err != nil {
				t.Errorf("read upload part: %v", err)
				return
			}
			if part.FormName() != "upload" || !strings.HasPrefix(part.FileName(), "SPEED_") {
				t.Error("invalid upload part")
			}
			n, err := io.Copy(io.Discard, part)
			if err != nil {
				return
			}
			if _, err := reader.NextPart(); err != io.EOF {
				t.Errorf("multipart terminator: %v", err)
				return
			}
			node.uploaded.Add(n)
			_ = waitProbe(r.Context(), time.Millisecond)
			_, _ = fmt.Fprint(w, "0")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(node.server.Close)
	client := New(WithUserConfig(&UserConfig{DeviceID: measurementDeviceID, Bandwidth: 500, MaxConnections: 1}))
	client.SetCaptureTime(60 * time.Millisecond).SetRateCaptureFrequency(5 * time.Millisecond)
	u, _ := url.Parse(node.server.URL)
	target := &Server{ID: node.server.URL, URL: node.server.URL + "/speed/doAnalsLoad.do?old=value", Host: u.Host, Context: client}
	return client, target, node
}

func (n *measurementNode) active(key string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.keys[key]
}

func (n *measurementNode) assertSessions(t *testing.T, count int) {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.issues != count || n.releases != count || len(n.keys) != 0 {
		t.Fatalf("sessions issued=%d released=%d active=%d want=%d", n.issues, n.releases, len(n.keys), count)
	}
}

func TestBackendMeasurementRepeatedPhases(t *testing.T) {
	client, target, node := newMeasurementFixture(t)
	var downloadCallbacks, uploadCallbacks atomic.Int64
	client.SetCallbackDownload(func(ByteRate) { downloadCallbacks.Add(1) })
	client.SetCallbackUpload(func(ByteRate) { uploadCallbacks.Add(1) })
	for i := 0; i < 2; i++ {
		if err := target.DownloadTestContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		if client.GetTotalDownload() <= 0 || target.DLSpeed <= 0 || target.TestDuration.Download == nil {
			t.Fatal("download measurement missing")
		}
		if err := target.UploadTestContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		if client.GetTotalUpload() <= 0 || target.ULSpeed <= 0 || target.TestDuration.Upload == nil {
			t.Fatal("upload measurement missing")
		}
	}
	node.assertSessions(t, 4)
	if downloadCallbacks.Load() < 2 || uploadCallbacks.Load() < 2 {
		t.Fatalf("callbacks down=%d up=%d", downloadCallbacks.Load(), uploadCallbacks.Load())
	}
	if client.GetTotalUpload() > node.uploaded.Load() {
		t.Fatal("confirmed more bytes than received payload")
	}
	client.Reset()
	if snapshot := client.Snapshots().Latest(); snapshot == nil || len(*snapshot) == 0 {
		t.Fatal("phase snapshots missing")
	}
	if target.TestDuration.Total == nil || *target.TestDuration.Total != *target.TestDuration.Download+*target.TestDuration.Upload {
		t.Fatal("total duration does not cover both directions")
	}
}

func TestBackendDownloadCountsDecodedPayload(t *testing.T) {
	client, target, node := newMeasurementFixture(t)
	var requests atomic.Int64
	node.hook = func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/speed/File(1G).dl" {
			return false
		}
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusOK)
			for _, part := range []string{"first", "second"} {
				_, _ = io.WriteString(w, part)
				w.(http.Flusher).Flush()
			}
		} else {
			<-r.Context().Done()
		}
		return true
	}
	if err := target.DownloadTestContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := client.GetTotalDownload(); got != int64(len("firstsecond")) {
		t.Fatalf("download bytes=%d", got)
	}
	node.assertSessions(t, 1)
}

func TestBackendMeasurementRejectsResponses(t *testing.T) {
	for _, upload := range []bool{false, true} {
		t.Run(fmt.Sprintf("upload=%v", upload), func(t *testing.T) {
			client, target, node := newMeasurementFixture(t)
			var requests atomic.Int64
			node.hook = func(w http.ResponseWriter, r *http.Request) bool {
				if (upload && r.URL.Path == "/speed/doAnalsLoad.do") || (!upload && r.URL.Path == "/speed/File(1G).dl") {
					requests.Add(1)
					_, _ = io.Copy(io.Discard, r.Body)
					w.WriteHeader(http.StatusForbidden)
					_, _ = fmt.Fprint(w, "rejected")
					return true
				}
				return false
			}
			var err error
			if upload {
				err = target.UploadTestContext(context.Background())
			} else {
				err = target.DownloadTestContext(context.Background())
			}
			if err == nil || !strings.Contains(err.Error(), "403") {
				t.Fatalf("error=%v", err)
			}
			if requests.Load() != 1 {
				t.Fatalf("retried rejected request %d times", requests.Load())
			}
			if client.GetTotalUpload() != 0 || client.GetTotalDownload() != 0 {
				t.Fatal("counted rejected response")
			}
			node.assertSessions(t, 1)
		})
	}
}

func TestBackendSessionRequiresCanonicalSuccess(t *testing.T) {
	for _, answer := range []string{"0", "2", "-1-", "1-not-a-uuid", "<html>error</html>", "1-00000000-0000-4000-8000-000000000001\nextra"} {
		t.Run(answer, func(t *testing.T) {
			client, target, node := newMeasurementFixture(t)
			node.hook = func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path == "/speed/dovalid" && r.Method == http.MethodGet {
					_, _ = fmt.Fprint(w, answer)
					return true
				}
				return false
			}
			if err := target.DownloadTestContext(context.Background()); err == nil {
				t.Fatal("accepted invalid issuance")
			}
			if node.downloads.Load() != 0 || client.GetTotalDownload() != 0 {
				t.Fatal("started transfer without session")
			}
			node.assertSessions(t, 0)
		})
	}
}

func TestBackendMeasurementCancellationReleasesSession(t *testing.T) {
	for _, upload := range []bool{false, true} {
		t.Run(fmt.Sprintf("upload=%v", upload), func(t *testing.T) {
			client, target, node := newMeasurementFixture(t)
			client.SetCaptureTime(time.Hour)
			started := make(chan struct{})
			var once sync.Once
			node.hook = func(w http.ResponseWriter, r *http.Request) bool {
				if (upload && r.URL.Path == "/speed/doAnalsLoad.do") || (!upload && r.URL.Path == "/speed/File(1G).dl") {
					if upload {
						_, _ = io.Copy(io.Discard, r.Body)
					}
					once.Do(func() { close(started) })
					<-r.Context().Done()
					return true
				}
				return false
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if upload {
					done <- target.UploadTestContext(ctx)
				} else {
					done <- target.DownloadTestContext(ctx)
				}
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("request did not start")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error=%v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation did not stop phase")
			}
			node.assertSessions(t, 1)
		})
	}
}

func TestBackendMultiMeasurementReleasesEveryNode(t *testing.T) {
	for _, upload := range []bool{false, true} {
		t.Run(fmt.Sprintf("upload=%v", upload), func(t *testing.T) {
			client, main, first := newMeasurementFixture(t)
			_, second, other := newMeasurementFixture(t)
			second.Context = client
			client.SetNThread(2)
			servers := Servers{main, second}
			var err error
			if upload {
				err = main.MultiUploadTestContext(context.Background(), servers)
			} else {
				err = main.MultiDownloadTestContext(context.Background(), servers)
			}
			if err != nil {
				t.Fatal(err)
			}
			first.assertSessions(t, 1)
			other.assertSessions(t, 1)
			if upload && (first.uploaded.Load() == 0 || other.uploaded.Load() == 0) {
				t.Fatal("missing node uploads")
			}
			if !upload && (first.downloads.Load() == 0 || other.downloads.Load() == 0) {
				t.Fatal("missing node downloads")
			}
		})
	}
}

func TestBackendMultiMeasurementCleansUpFailedAcquisition(t *testing.T) {
	client, main, first := newMeasurementFixture(t)
	_, second, other := newMeasurementFixture(t)
	second.Context = client
	client.SetNThread(2)
	other.hook = func(w http.ResponseWriter, r *http.Request) bool { _, _ = fmt.Fprint(w, "0"); return true }
	if err := main.MultiDownloadTestContext(context.Background(), Servers{main, second}); !errors.Is(err, caict.ErrBusy) {
		t.Fatalf("error=%v", err)
	}
	first.assertSessions(t, 1)
	if first.downloads.Load() != 0 {
		t.Fatal("transfer started before all sessions acquired")
	}
}

func TestBackendMeasurementUsesConfiguredProxy(t *testing.T) {
	client, target, node := newMeasurementFixture(t)
	var requests atomic.Int64
	forward := http.DefaultTransport.(*http.Transport).Clone()
	forward.Proxy = nil
	defer forward.CloseIdleConnections()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		clone := r.Clone(r.Context())
		clone.RequestURI = ""
		resp, err := forward.RoundTrip(clone)
		if err != nil {
			http.Error(w, "proxy", http.StatusBadGateway)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	defer proxy.Close()
	client.NewUserConfig(&UserConfig{DeviceID: measurementDeviceID, Bandwidth: 500, Proxy: proxy.URL, Source: "127.0.0.1", MaxConnections: 1})
	if err := target.DownloadTestContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	node.assertSessions(t, 1)
	if requests.Load() < 3 {
		t.Fatalf("proxy requests=%d", requests.Load())
	}
	if !client.config.T.DisableCompression || client.tcpDialer.LocalAddr == nil {
		t.Fatal("transport/source configuration missing")
	}
}

type measurementRoundTripper func(*http.Request) (*http.Response, error)

func (f measurementRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestBackendUploadRequiresCompleteAcknowledgment(t *testing.T) {
	for _, mode := range []string{"unread request", "truncated response"} {
		t.Run(mode, func(t *testing.T) {
			client, target, node := newMeasurementFixture(t)
			base := client.doer
			custom := &http.Client{Transport: measurementRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/speed/doAnalsLoad.do" {
					return base.Do(r)
				}
				if mode == "truncated response" {
					_, _ = io.Copy(io.Discard, r.Body)
				}
				body := io.ReadCloser(io.NopCloser(strings.NewReader("ok")))
				if mode == "truncated response" {
					body = &measurementFailedBody{}
				}
				return &http.Response{StatusCode: 200, Body: body, Header: make(http.Header), Request: r}, nil
			})}
			client.doer = custom
			if err := target.UploadTestContext(context.Background()); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("error=%v", err)
			}
			if client.GetTotalUpload() != 0 {
				t.Fatal("counted unacknowledged upload")
			}
			node.assertSessions(t, 1)
		})
	}
}

type measurementFailedBody struct{}

func (*measurementFailedBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (*measurementFailedBody) Close() error             { return nil }
