package speedtest

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestHTTPPingRootResponses(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNotFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var requests atomic.Int64
			node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.URL.Path != "/" || r.URL.RawQuery != "" {
					t.Errorf("ping URL = %s", r.URL)
				}
				w.WriteHeader(status)
			}))
			defer node.Close()
			target := &Server{URL: node.URL + "/speed/doAnalsLoad.do?old=value", Context: New()}
			var callbacks int
			latencies, err := target.HTTPPing(context.Background(), 2, 0, func(time.Duration) { callbacks++ })
			if status >= 500 {
				if !errors.Is(err, ErrConnectTimeout) || len(latencies) != 0 {
					t.Fatalf("samples=%v error=%v", latencies, err)
				}
			} else if err != nil || len(latencies) != 2 || callbacks != 2 {
				t.Fatalf("samples=%v callbacks=%d error=%v", latencies, callbacks, err)
			}
			if requests.Load() != 2 {
				t.Fatalf("requests=%d", requests.Load())
			}
		})
	}
}

func TestPingTestPublishesActualProbeCounts(t *testing.T) {
	var requests atomic.Int64
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer node.Close()
	target := &Server{URL: node.URL, Context: New()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := target.PingTestContext(ctx, func(time.Duration) { cancel() }); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if target.PacketLoss.Sent != 1 || target.PacketLoss.Max != 1 || target.PacketLoss.Loss() != 0.5 {
		t.Fatalf("probe summary=%+v", target.PacketLoss)
	}
	if target.TestDuration.Ping == nil || target.Latency <= 0 {
		t.Fatal("partial ping measurements missing")
	}
	if _, err := target.HTTPPing(context.Background(), 1, 0, nil); err != nil {
		t.Fatal(err)
	}
	if target.PacketLoss.Sent != 1 || target.PacketLoss.Max != 1 {
		t.Fatal("loaded latency probe changed initial summary")
	}
}

func TestHTTPPingCancellationInterruptsInterval(t *testing.T) {
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer node.Close()
	target := &Server{URL: node.URL, Context: New()}
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	samples, err := target.HTTPPing(ctx, 10, time.Hour, func(time.Duration) { cancel() })
	if !errors.Is(err, context.Canceled) || len(samples) != 1 || time.Since(start) > time.Second {
		t.Fatalf("samples=%v error=%v duration=%v", samples, err, time.Since(start))
	}
}

func TestTCPPingUsesHandshakeAndConfiguredDialer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	unexpected := make(chan error, 2)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := listener.Accept()
			if err != nil {
				unexpected <- err
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			var data [1]byte
			n, _ := conn.Read(data[:])
			_ = conn.Close()
			if n != 0 {
				unexpected <- errors.New("TCP ping sent application data")
			} else {
				unexpected <- nil
			}
		}
	}()
	var dials atomic.Int64
	client := New(WithUserConfig(&UserConfig{Source: "127.0.0.1"}))
	client.tcpDialer.Control = func(_, _ string, _ syscall.RawConn) error { dials.Add(1); return nil }
	target := &Server{URL: "http://" + listener.Addr().String() + "/speed/doAnalsLoad.do", Host: listener.Addr().String(), Context: client}
	samples, err := target.TCPPing(context.Background(), 2, 0, nil)
	if err != nil || len(samples) != 2 || dials.Load() != 2 {
		t.Fatalf("samples=%v dials=%d error=%v", samples, dials.Load(), err)
	}
	for i := 0; i < 2; i++ {
		if err := <-unexpected; err != nil {
			t.Fatal(err)
		}
	}
}

func TestDownloadTestContext(t *testing.T) {
	client, target, _ := newMeasurementFixture(t)
	client.SetNThread(1).SetCaptureTime(50 * time.Millisecond).SetRateCaptureFrequency(time.Millisecond)
	err := target.downloadTestContext(context.Background(), mockRequest)
	if err != nil {
		t.Fatal(err)
	}
	if got := client.GetAvgDownloadRate(); got <= 0 {
		t.Fatalf("download rate=%v", got)
	}
	if target.TestDuration.Download == nil || *target.TestDuration.Download != *target.TestDuration.Total {
		t.Fatal("download duration missing")
	}
}

func TestUploadTestContext(t *testing.T) {
	client, target, _ := newMeasurementFixture(t)
	client.SetNThread(1).SetCaptureTime(50 * time.Millisecond).SetRateCaptureFrequency(time.Millisecond)
	err := target.uploadTestContext(context.Background(), mockRequest)
	if err != nil {
		t.Fatal(err)
	}
	if got := client.GetAvgUploadRate(); got <= 0 {
		t.Fatalf("upload rate=%v", got)
	}
	if target.TestDuration.Upload == nil || *target.TestDuration.Upload != *target.TestDuration.Total {
		t.Fatal("upload duration missing")
	}
}

func mockRequest(ctx context.Context, s *Server, _ int) error {
	s.Context.AddTotalDownload(1000)
	s.Context.AddTotalUpload(1000)
	return waitProbe(ctx, 5*time.Millisecond)
}

func TestPautaFilter(t *testing.T) {
	_, _, std, _, _ := StandardDeviation([]int64{26, 23, 32})
	if std != 3 {
		t.Fatalf("standard deviation=%d", std)
	}
	if got := pautaFilter([]int64{3, 4, 5, 6, 6, 6, 1, 7, 9, 5, 200}); len(got) != 10 {
		t.Fatalf("filtered=%v", got)
	}
}
