package caict

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const validationUUID = "01234567-89ab-4cde-8fab-0123456789ab"

func validationServer(t *testing.T, addr string) Server {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	return Server{HostIP: host, Port: port}
}

// Each raw fixture serves one loopback request and bounds its own lifetime.
func validationRawServer(t *testing.T, serve func(net.Conn)) Server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
			t.Errorf("read test request: %v", err)
			return
		}
		serve(conn)
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Error("raw fixture did not stop")
		}
	})
	return validationServer(t, ln.Addr().String())
}

func TestResponseValidationIssuance(t *testing.T) {
	for _, tc := range []struct {
		name, body, key string
		cause           error
	}{
		{"success", " \r\n1-" + validationUUID + "\n\t", validationUUID, nil},
		{"uppercase", "1-" + strings.ToUpper(validationUUID), strings.ToUpper(validationUUID), nil},
		{"busy", " \n0\t", "", ErrBusy},
		{"busy delimiter", "0-", "", ErrBusy},
		{"retry", "2", "", ErrRetry},
		{"retry delimiter", "2-", "", ErrRetry},
		{"rejected", " -1\r\n", "", ErrRejected},
		{"rejected delimiter", "\t-1-\n", "", ErrRejected},
		{"empty", " \n", "", ErrInvalidResponse},
		{"html", "<html>node error</html>", "", ErrInvalidResponse},
		{"arbitrary text", "hello", "", ErrInvalidResponse},
		{"wrong prefix", "9-" + validationUUID, "", ErrInvalidResponse},
		{"missing prefix", validationUUID, "", ErrInvalidResponse},
		{"short uuid", "1-01234567-89ab-4cde-8fab-0123456789a", "", ErrInvalidResponse},
		{"long groups", "1-01234567-89ab-44cde-98fab-0123456789ab", "", ErrInvalidResponse},
		{"no hyphens", "1-0123456789ab4cde8fab0123456789ab", "", ErrInvalidResponse},
		{"non hex", "1-01234567-89ab-4cde-8fab-0123456789ag", "", ErrInvalidResponse},
		{"extra line", "1-" + validationUUID + "\nextra", "", ErrInvalidResponse},
		{"retry prefix", "200", "", ErrInvalidResponse},
		{"reject prefix", "-10", "", ErrInvalidResponse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, err := issueResponse(tc.body)
			if key != tc.key || !errors.Is(err, tc.cause) {
				t.Fatalf("got key %q, error %v; want key %q, error %v", key, err, tc.key, tc.cause)
			}
			if tc.cause != nil {
				var responseErr *DovalidError
				if !errors.As(err, &responseErr) || responseErr.Response != strings.TrimSpace(tc.body) {
					t.Fatalf("trimmed response not preserved: %#v", responseErr)
				}
			}
		})
	}
	_, err := issueResponse("<html>token=synthetic-secret</html>")
	if strings.Contains(err.Error(), "synthetic-secret") {
		t.Fatal("error text includes arbitrary response content")
	}
}

func TestResponseValidationIssueSessionHTTP(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		cause      error
		ok         bool
	}{
		{"success", "1-" + validationUUID, 200, nil, true},
		{"busy", "0", 200, ErrBusy, false},
		{"retry", "2", 200, ErrRetry, false},
		{"rejected", "-1-", 200, ErrRejected, false},
		{"html", "<html>error</html>", 200, ErrInvalidResponse, false},
		{"bad uuid", "1-not-a-uuid", 200, ErrInvalidResponse, false},
		{"server error with valid looking body", "1-" + validationUUID, 500, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/speed/dovalid" || r.URL.Query().Get("key") != "" || r.URL.Query().Get("imei") != "TS0000000000000000" {
					t.Error("unexpected issuance request")
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer s.Close()
			key, _, err := IssueSession(context.Background(), validationServer(t, s.Listener.Addr().String()), Session{
				IMEI: "TS0000000000000000", Model: "iphone", Pkg: DefaultPkg, Bandwidth: 100,
			})
			if tc.ok {
				if err != nil || key != validationUUID {
					t.Fatalf("issuance: key %q, error %v", key, err)
				}
			} else if err == nil || key != "" {
				t.Fatalf("invalid issuance accepted: key %q, error %v", key, err)
			} else if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatalf("got %v, want %v", err, tc.cause)
			}
		})
	}
}

func TestResponseValidationHTTPGet(t *testing.T) {
	originalClient, originalTimeout := http.DefaultClient, http.DefaultClient.Timeout
	for _, tc := range []struct {
		name, body string
		status     int
		ok         bool
	}{
		{"ok", "test", 200, true},
		{"other 2xx", "test", 201, true},
		{"empty 2xx", "", 204, true},
		{"forbidden", "<html>forbidden</html>", 403, false},
		{"server error", "error", 500, false},
		{"oversized", strings.Repeat("x", (1<<16)+1), 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer s.Close()
			body, err := httpGetString(context.Background(), s.URL+"?token=synthetic-secret", ieUA)
			if (err == nil) != tc.ok || (tc.ok && body != tc.body) {
				t.Fatalf("got %d bytes, error %v; want success %v", len(body), err, tc.ok)
			}
			if err != nil && strings.Contains(err.Error(), "synthetic-secret") {
				t.Fatal("HTTP error exposes request secret")
			}
		})
	}
	if http.DefaultClient != originalClient || http.DefaultClient.Timeout != originalTimeout {
		t.Fatal("global HTTP client changed")
	}
	t.Run("truncated body", func(t *testing.T) {
		srv := validationRawServer(t, func(conn net.Conn) {
			_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc")
		})
		body, err := httpGetString(context.Background(), "http://"+srv.Addr(), ieUA)
		if err == nil || body != "" || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("truncated response: body %q, error %v", body, err)
		}
	})
	t.Run("cancellation hides request URL", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := httpGetString(ctx, "http://127.0.0.1:1/?imei=synthetic-device&token=synthetic-secret", ieUA)
		if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "synthetic-") {
			t.Fatalf("canceled request: %v", err)
		}
	})
}

func TestResponseValidationDownloadProbe(t *testing.T) {
	for _, tc := range []struct {
		name, wire string
		bytes      int64
		status     int
		ok         bool
		cause      error
	}{
		{"200", "HTTP/1.1 200 OK\r\nContent-Length: 16\r\n\r\n0123456789abcdef", 8, 200, true, nil},
		{"206", "HTTP/1.1 206 Partial Content\r\nContent-Length: 8\r\n\r\n01234567", 8, 206, true, nil},
		{"chunked counts payload", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n5\r\ndefgh\r\n0\r\n\r\n", 8, 200, true, nil},
		{"403", "HTTP/1.1 403 Forbidden\r\nContent-Length: 8\r\n\r\nforbid!!", 0, 403, false, ErrRejected},
		{"500", "HTTP/1.1 500 Error\r\nContent-Length: 8\r\n\r\nerror!!!", 0, 500, false, nil},
		{"204", "HTTP/1.1 204 No Content\r\n\r\n", 0, 204, false, nil},
		{"redirect", "HTTP/1.1 302 Found\r\nContent-Length: 8\r\n\r\nredirect", 0, 302, false, nil},
		{"empty response", "", 0, 0, false, nil},
		{"html without HTTP", "<html>error</html>", 0, 0, false, nil},
		{"malformed status", "HTTP/1.1 abc Bad\r\n\r\n01234567", 0, 0, false, nil},
		{"malformed header", "HTTP/1.1 200 OK\r\ninvalid header\r\n\r\n01234567", 0, 0, false, nil},
		{"incomplete header", "HTTP/1.1 200 OK\r\nContent-Length: 8\r\n", 0, 0, false, nil},
		{"oversized header", "HTTP/1.1 200 OK\r\nX-Test: " + strings.Repeat("x", 65<<10) + "\r\n\r\n01234567", 0, 0, false, nil},
		{"empty body", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", 0, 200, false, io.EOF},
		{"short declared body", "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc", 3, 200, false, io.ErrUnexpectedEOF},
		{"truncated body", "HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nabc", 3, 200, false, io.ErrUnexpectedEOF},
		{"short EOF body", "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nabc", 3, 200, false, io.ErrUnexpectedEOF},
		{"invalid chunk", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n01234567", 0, 200, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := validationRawServer(t, func(conn net.Conn) { _, _ = io.WriteString(conn, tc.wire) })
			got, status, err := DownloadProbe(context.Background(), srv, validationUUID, 8)
			if got != tc.bytes || status != tc.status || (err == nil) != tc.ok {
				t.Fatalf("got %d bytes, HTTP %d, error %v; want %d bytes, HTTP %d, success %v", got, status, err, tc.bytes, tc.status, tc.ok)
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatalf("got %v, want %v", err, tc.cause)
			}
		})
	}
	for _, n := range []int64{0, -1} {
		if _, _, err := DownloadProbe(context.Background(), Server{}, validationUUID, n); err == nil {
			t.Errorf("size %d accepted", n)
		}
	}
}

func TestResponseValidationProbeCancellation(t *testing.T) {
	for _, stage := range []string{"headers", "body"} {
		for _, deadline := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deadline=%v", stage, deadline), func(t *testing.T) {
				ready, peerClosed := make(chan struct{}), make(chan struct{})
				srv := validationRawServer(t, func(conn net.Conn) {
					if stage == "body" {
						_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nab")
					}
					close(ready)
					_, _ = io.Copy(io.Discard, conn)
					close(peerClosed)
				})
				ctx, cancel := context.WithCancel(context.Background())
				want := error(context.Canceled)
				if deadline {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 150*time.Millisecond)
					want = context.DeadlineExceeded
				}
				defer cancel()
				done := make(chan error, 1)
				go func() {
					_, _, err := DownloadProbe(ctx, srv, validationUUID, 8)
					done <- err
				}()
				select {
				case <-ready:
				case <-time.After(time.Second):
					t.Fatal("probe did not reach server")
				}
				if !deadline {
					cancel()
				}
				select {
				case err := <-done:
					if !errors.Is(err, want) {
						t.Fatalf("got %v, want %v", err, want)
					}
				case <-time.After(time.Second):
					t.Fatal("probe did not honor cancellation/deadline")
				}
				select {
				case <-peerClosed:
				case <-time.After(time.Second):
					t.Fatal("probe socket remained open")
				}
			})
		}
	}
}

func TestResponseValidationMatchServersNil(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, field := range []string{"ip", "province", "city", "wifioper"} {
			if r.URL.Query().Get(field) != "" {
				t.Errorf("unexpected value for %s without a location", field)
			}
		}
		_, _ = io.WriteString(w, `[]`)
	}))
	defer s.Close()
	c := &ControlPlane{Base: s.URL, HTTP: s.Client()}
	if servers, err := c.MatchServers(context.Background(), nil, false); err != nil || len(servers) != 0 {
		t.Fatalf("nil location: %v, %v", servers, err)
	}
}
