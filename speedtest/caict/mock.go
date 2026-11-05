package caict

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MockNode re-implements the observed behavior of a real speed test node
// (Java service on port 65499/9443) for end-to-end validation without
// touching production infrastructure:
//
//   - GET /speed/dovalid?key=&flag=true&...  → "1-{uuid}" session issuance
//     (empty key = enqueue, like production); non-empty issued key → "0";
//     unknown non-empty key → "-1-"
//   - GET /speed/File(1G).dl?key={uuid}      → 403 CAICT-styled body unless
//     the uuid was issued; else a synthetic payload stream
//   - POST /speed/doAnalsLoad.do (Key: {uuid}) → 403 or 200 + byte count
//   - POST /speed/dovalid?key={uuid}         → session end (dequeue)
type MockNode struct {
	Payload int64 // bytes served per download request
	srv     *http.Server
	ln      net.Listener

	mu       sync.Mutex
	issued   map[string]bool
	uploaded atomic.Uint64
	downhits atomic.Uint64
}

// NewMockNode starts a loopback mock node; use Addr() to point a client at
// it. Sessions are issued via the empty-key dovalid handshake exactly like
// production — there is no static key.
func NewMockNode(payload int64) (*MockNode, error) {
	if payload <= 0 {
		payload = 64 << 20
	}
	m := &MockNode{Payload: payload, issued: make(map[string]bool)}
	mux := http.NewServeMux()
	mux.HandleFunc("/speed/dovalid", m.handleDoValid)
	mux.HandleFunc("/speed/File(1G).dl", m.handleDownload)
	mux.HandleFunc("/speed/doAnalsLoad.do", m.handleUpload)
	mux.HandleFunc("/speed/upload", m.handleUpload) // legacy alias

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	m.ln = ln
	m.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = m.srv.Serve(ln) }()
	return m, nil
}

func (m *MockNode) Addr() string { return m.ln.Addr().String() }

func (m *MockNode) Server() Server {
	host, port, _ := net.SplitHostPort(m.Addr())
	return Server{
		HostID: "0", HostIP: host, Port: port,
		HostName: "mock-CAICT-node", Location: "本地", Oper: "mock", PName: "loopback",
	}
}

func (m *MockNode) Uploaded() uint64     { return m.uploaded.Load() }
func (m *MockNode) DownloadHits() uint64 { return m.downhits.Load() }

// Close shuts the mock down (safe for tests via t.Cleanup).
func (m *MockNode) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return m.srv.Shutdown(ctx)
}

func (m *MockNode) known(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.issued[key]
}

func (m *MockNode) handleDoValid(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		q := r.URL.Query()
		bandwidth, err := strconv.Atoi(q.Get("bandwidth"))
		if r.Method != http.MethodGet || q.Get("flag") != "true" || err != nil ||
			q.Get("imei") == "" || q.Get("time") == "" || q.Get("model") == "" ||
			q.Get("token") != ComputeToken(q.Get("model"), q.Get("imei"), q.Get("time"), bandwidth) {
			_, _ = io.WriteString(w, "-1-")
			return
		}
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			http.Error(w, "random source failed", http.StatusInternalServerError)
			return
		}
		b[6] = (b[6] & 0x0f) | 0x40
		b[8] = (b[8] & 0x3f) | 0x80
		h := hex.EncodeToString(b)
		uuid := h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
		m.mu.Lock()
		m.issued[uuid] = true
		m.mu.Unlock()
		_, _ = io.WriteString(w, "1-"+uuid)
		return
	}
	if m.known(key) {
		if r.Method == http.MethodPost {
			m.mu.Lock()
			delete(m.issued, key)
			m.mu.Unlock()
		}
		_, _ = io.WriteString(w, "0")
		return
	}
	_, _ = io.WriteString(w, "-1-")
}

func (m *MockNode) keyOf(r *http.Request) string {
	if k := r.URL.Query().Get("key"); k != "" {
		return k
	}
	return r.Header.Get("Key")
}

func (m *MockNode) forbid(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/html;charset=UTF-8")
	w.WriteHeader(http.StatusForbidden)
	_, _ = io.WriteString(w, "<html><body><div style='text-align:center'><h1 style='margin-top: 10px;'>403 Forbidden!</h1><hr><span>caict-mock</span></div></body></html>")
}

func (m *MockNode) handleDownload(w http.ResponseWriter, r *http.Request) {
	if !m.known(m.keyOf(r)) {
		m.forbid(w)
		return
	}
	m.downhits.Add(1)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(m.Payload, 10))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
	// serve zeros from a shared 1MiB buffer; speed, not entropy, matters here.
	buf := make([]byte, 1<<20)
	n := m.Payload
	for n > 0 {
		chunk := int64(len(buf))
		if n < chunk {
			chunk = n
		}
		wn, err := w.Write(buf[:chunk])
		if err != nil {
			return
		}
		n -= int64(wn)
	}
}

func (m *MockNode) handleUpload(w http.ResponseWriter, r *http.Request) {
	if !m.known(m.keyOf(r)) {
		m.forbid(w)
		return
	}
	n, err := io.Copy(io.Discard, r.Body)
	if err != nil && !strings.Contains(err.Error(), "unexpected EOF") {
		log.Printf("caict-mock: upload read: %v", err)
	}
	m.uploaded.Add(uint64(n))
	_, _ = io.WriteString(w, "0")
}
