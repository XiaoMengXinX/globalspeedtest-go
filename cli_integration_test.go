package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/XiaoMengXinX/globalspeedtest-go/speedtest/caict"
)

func TestCLIProcess(t *testing.T) {
	if os.Getenv("GLOBALSPEEDTEST_CLI_TEST") != "1" {
		return
	}
	args := strings.Split(os.Getenv("GLOBALSPEEDTEST_CLI_ARGS"), "\n")
	os.Args = append([]string{"globalspeedtest-go"}, args...)
	main()
	os.Exit(0)
}

type cliFixture struct {
	server   *httptest.Server
	mu       sync.Mutex
	issues   int
	releases int
	badToken bool
	download chan struct{}
}

func newCLIFixture(t *testing.T) *cliFixture {
	t.Helper()
	f := &cliFixture{download: make(chan struct{}, 1)}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dataServer/getIpLocSP.php":
			_, _ = io.WriteString(w, `192.0.2.10|["中国","辽宁","大连","","联通"]|2|联通|100`)
		case "/dataServer/mobilematch_many.php":
			host, port, _ := net.SplitHostPort(r.Host)
			_ = json.NewEncoder(w).Encode([]map[string]string{{
				"hostid": "12341", "hostip": host, "port": port, "hostname": "大连测试节点",
				"pname": "辽宁", "city": "大连", "oper": "联通",
			}})
		case "/speed/dovalid":
			q := r.URL.Query()
			f.mu.Lock()
			defer f.mu.Unlock()
			if r.Method == http.MethodPost {
				f.releases++
				_, _ = io.WriteString(w, "0")
				return
			}
			if q.Get("key") != "" {
				http.Error(w, "unexpected key", http.StatusBadRequest)
				return
			}
			var band int
			_, _ = fmt.Sscanf(q.Get("bandwidth"), "%d", &band)
			if q.Get("token") != caict.ComputeToken(q.Get("model"), q.Get("imei"), q.Get("time"), band) {
				f.badToken = true
				_, _ = io.WriteString(w, "-1-")
				return
			}
			f.issues++
			_, _ = io.WriteString(w, "1-01234567-89ab-4cde-8f01-23456789abcd")
		case "/speed/File(1G).dl":
			select {
			case f.download <- struct{}{}:
			default:
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			buf := make([]byte, 32<<10)
			for i := 0; i < 256; i++ {
				if _, err := w.Write(buf); err != nil {
					return
				}
				select {
				case <-r.Context().Done():
					return
				case <-time.After(time.Millisecond):
				}
			}
		case "/speed/doAnalsLoad.do":
			if r.Method != http.MethodPost || r.Header.Get("Key") == "" {
				http.Error(w, "missing session", http.StatusForbidden)
				return
			}
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				return
			}
			_, _ = io.WriteString(w, "0")
		case "/":
			_, _ = io.WriteString(w, "ok")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func cliCommand(t *testing.T, fixture *cliFixture, args ...string) *exec.Cmd {
	t.Helper()
	defaults := []string{"--control-url", fixture.server.URL, "--thread", "1"}
	if !containsString(args, "--duration") {
		defaults = append(defaults, "--duration", "300ms")
	}
	args = append(defaults, args...)
	cmd := exec.Command(os.Args[0], "-test.run=^TestCLIProcess$")
	cmd.Env = append(os.Environ(), "GLOBALSPEEDTEST_CLI_TEST=1", "GLOBALSPEEDTEST_CLI_ARGS="+strings.Join(args, "\n"))
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

func TestCLIJSONUsesGlobalspeedBackend(t *testing.T) {
	f := newCLIFixture(t)
	cmd := cliCommand(t, f, "--json", "--unix")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, stderr.String())
	}
	var result struct {
		Servers []struct {
			ID      string  `json:"id"`
			DLSpeed float64 `json:"dl_speed"`
			ULSpeed float64 `json:"ul_speed"`
		} `json:"servers"`
	}
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("JSON: %v\n%s", err, out)
	}
	if len(result.Servers) != 1 || result.Servers[0].ID != "12341" {
		t.Fatalf("servers: %+v", result.Servers)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.issues != 2 || f.releases != 2 || f.badToken {
		t.Fatalf("issues=%d releases=%d badToken=%v", f.issues, f.releases, f.badToken)
	}
	if bytes.Contains(out, []byte("01234567-89ab")) {
		t.Fatal("session key in output")
	}
}

func TestCLITerminalRendersMeasurementTasks(t *testing.T) {
	f := newCLIFixture(t)
	cmd := cliCommand(t, f)
	cmd.Env = append(cmd.Env, "YSMRR_FORCE_TTY=true")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, text := range []string{"CAICT", "Download:", "Upload:", "\x1b[?25l", "\x1b[?25h"} {
		if !bytes.Contains(out, []byte(text)) {
			t.Fatalf("missing %q\n%s", text, out)
		}
	}
}

func TestCLICancelReleasesSession(t *testing.T) {
	for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			f := newCLIFixture(t)
			cmd := cliCommand(t, f, "--duration", "10s", "--no-upload")
			cmd.Env = append(cmd.Env, "YSMRR_FORCE_TTY=true")
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if cmd.ProcessState == nil {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			}()
			select {
			case <-f.download:
			case <-time.After(5 * time.Second):
				t.Fatal("download did not start")
			}
			if err := cmd.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Wait(); err == nil {
				t.Fatal("canceled measurement exited successfully")
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.issues != 1 || f.releases != 1 {
				t.Fatalf("issues=%d releases=%d\n%s", f.issues, f.releases, stderr.String())
			}
			if !bytes.Contains(stdout.Bytes(), []byte("\x1b[?25h")) {
				t.Fatal("cursor was not restored")
			}
		})
	}
}

func newSpeedNode(t *testing.T, downloadStatus, uploadStatus int, pingDelay time.Duration) (*httptest.Server, func() (issues, releases int)) {
	t.Helper()
	var mu sync.Mutex
	var issues, releases int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/speed/dovalid":
			mu.Lock()
			defer mu.Unlock()
			if r.Method == http.MethodPost {
				releases++
				_, _ = io.WriteString(w, "0")
				return
			}
			var band int
			_, _ = fmt.Sscanf(r.URL.Query().Get("bandwidth"), "%d", &band)
			q := r.URL.Query()
			if q.Get("token") != caict.ComputeToken(q.Get("model"), q.Get("imei"), q.Get("time"), band) {
				_, _ = io.WriteString(w, "-1-")
				return
			}
			issues++
			_, _ = io.WriteString(w, "1-01234567-89ab-4cde-8f01-23456789abcd")
		case "/speed/File(1G).dl":
			if downloadStatus != http.StatusOK {
				w.WriteHeader(downloadStatus)
				return
			}
			buf := make([]byte, 32<<10)
			for i := 0; i < 256; i++ {
				if _, err := w.Write(buf); err != nil {
					return
				}
				select {
				case <-r.Context().Done():
					return
				case <-time.After(time.Millisecond):
				}
			}
		case "/speed/doAnalsLoad.do":
			if r.Method != http.MethodPost || r.Header.Get("Key") == "" {
				http.Error(w, "missing session", http.StatusForbidden)
				return
			}
			if uploadStatus != http.StatusOK {
				w.WriteHeader(uploadStatus)
				return
			}
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = io.WriteString(w, "0")
		default:
			if pingDelay > 0 {
				time.Sleep(pingDelay)
			}
			_, _ = io.WriteString(w, "ok")
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() (int, int) { mu.Lock(); defer mu.Unlock(); return issues, releases }
}

// runCLIOnNodes runs an auto-selecting measurement over the nodes, listed
// lowest latency first.
func runCLIOnNodes(t *testing.T, nodes ...*httptest.Server) ([]byte, error) {
	t.Helper()
	var entries []map[string]string
	for i, srv := range nodes {
		host, port, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
		id := strconv.Itoa(i + 1)
		entries = append(entries, map[string]string{"hostid": id, "hostip": host, "port": port, "hostname": id, "pname": "辽宁", "city": "大连", "oper": "联通"})
	}
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dataServer/getIpLocSP.php":
			_, _ = io.WriteString(w, `192.0.2.10|["中国","辽宁","大连","","联通"]|2|联通|100`)
		case "/dataServer/mobilematch_many.php":
			_ = json.NewEncoder(w).Encode(entries)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(control.Close)

	cmd := exec.Command(os.Args[0], "-test.run=^TestCLIProcess$")
	args := []string{"--control-url", control.URL, "--thread", "1", "--duration", "300ms", "--unix"}
	cmd.Env = append(os.Environ(), "GLOBALSPEEDTEST_CLI_TEST=1", "GLOBALSPEEDTEST_CLI_ARGS="+strings.Join(args, "\n"))
	cmd.WaitDelay = 5 * time.Second
	return cmd.CombinedOutput()
}

func TestCLIFallsBackToNextServer(t *testing.T) {
	bad, badStats := newSpeedNode(t, http.StatusForbidden, http.StatusOK, 0)
	good, goodStats := newSpeedNode(t, http.StatusOK, http.StatusOK, 30*time.Millisecond)
	out, err := runCLIOnNodes(t, bad, good)
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte("Download:")) {
		t.Fatalf("no successful download after fallback\n%s", out)
	}
	badIssues, badReleases := badStats()
	if badIssues != 1 || badReleases != 1 {
		t.Fatalf("bad node issues=%d releases=%d", badIssues, badReleases)
	}
	goodIssues, goodReleases := goodStats()
	if goodIssues < 1 || goodReleases < 1 {
		t.Fatalf("good node issues=%d releases=%d", goodIssues, goodReleases)
	}
}

func TestCLIKeepsServerThatDeliveredData(t *testing.T) {
	first, _ := newSpeedNode(t, http.StatusOK, http.StatusForbidden, 0)
	second, secondStats := newSpeedNode(t, http.StatusOK, http.StatusOK, 30*time.Millisecond)
	out, err := runCLIOnNodes(t, first, second)
	if err == nil {
		t.Fatalf("the failed upload was not reported\n%s", out)
	}
	if bytes.Contains(out, []byte("Switching to the next server")) {
		t.Fatalf("switched servers after the download\n%s", out)
	}
	if issues, _ := secondStats(); issues != 0 {
		t.Fatalf("second node issues=%d", issues)
	}
}
