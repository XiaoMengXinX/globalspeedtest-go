package caict

import (
	"context"
	"crypto/des"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

func TestDecryptServerListFromBundlePayload(t *testing.T) {
	// First 64 hex-text chars of the real serverlist_encrypt.json (v4.4.9
	// IPA) = two DES ciphertext blocks, decrypting to the JSON head. A
	// truncated stream has no valid padding, so check the raw blocks.
	const prefix = "74dfdb9e3755c8d446a2d5305af1996e93bc2ae5269f87fec4029b1df055c5f3"
	raw, err := hex.DecodeString(prefix)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := desECBDecryptRaw([]byte(serverListKey), raw)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !strings.HasPrefix(string(plain), `[{"cityid": "48002001"`) {
		t.Fatalf("unexpected plaintext: %q", plain)
	}
}

func TestDESRoundTrip(t *testing.T) {
	block, err := des.NewCipher([]byte(serverListKey))
	if err != nil {
		t.Fatal(err)
	}
	msg := []byte(`[{"cityid": "48002001", "hostname": "上海电信"}]`)
	pad := 8 - len(msg)%8
	padded := append(append([]byte{}, msg...), make([]byte, pad)...)
	for i := len(msg); i < len(padded); i++ {
		padded[i] = byte(pad)
	}
	ct := make([]byte, len(padded))
	for off := 0; off < len(padded); off += 8 {
		block.Encrypt(ct[off:off+8], padded[off:off+8])
	}
	got, err := decryptServerList(hex.EncodeToString(ct))
	if err != nil {
		t.Fatalf("roundtrip: %v", err)
	}
	if string(got) != string(msg) {
		t.Fatalf("roundtrip mismatch: %q", got)
	}
}

func TestEmbeddedServerList(t *testing.T) {
	l, err := EmbeddedServerList()
	if err != nil {
		t.Fatalf("embedded list: %v", err)
	}
	if len(l.Servers) < 700 {
		t.Fatalf("expected ~722 nodes, got %d", len(l.Servers))
	}
	if got := l.Filter("电信", "", "上海"); len(got.Servers) == 0 {
		t.Fatalf("no 上海电信 nodes found")
	}
}

// Pin the independently calculated formula using a synthetic identity.
func TestComputeTokenVector(t *testing.T) {
	got := ComputeToken("iphone", "TS0123456789ABCDEF", "1700000000", 500)
	if got != "d70ffad3a2a8c761f2a2d055de1fdbf4" {
		t.Fatalf("token formula drifted: got %s", got)
	}
}

func TestParseIpLocAgainstCapture(t *testing.T) {
	loc, err := parseIpLoc("192.0.2.10|[\"中国\",\"辽宁\",\"\",\"\",\"联通\"]|2|联通|34447")
	if err != nil {
		t.Fatal(err)
	}
	if loc.OuterIP != "192.0.2.10" || loc.Region[1] != "辽宁" || loc.OperName != "联通" || loc.OperID != "2" {
		t.Fatalf("parse mismatch: %+v", loc)
	}
}

func TestIssueResponseCodes(t *testing.T) {
	uuid, err := issueResponse("1-01234567-89ab-4cde-8f01-23456789abcd\n")
	if err != nil || uuid != "01234567-89ab-4cde-8f01-23456789abcd" {
		t.Fatalf("issuance parse: %q %v", uuid, err)
	}
	if _, err := issueResponse("0"); err == nil {
		t.Fatal("busy must error")
	}
	if _, err := issueResponse("-1-"); err == nil {
		t.Fatal("reject must error")
	}
}

func TestMockRejectsUnknownKey(t *testing.T) {
	m, err := NewMockNode(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	srv := m.Server()

	// fabricated key → "-1-" rejection, matching production
	body, err := httpGetString(context.Background(),
		"http://"+srv.Addr()+"/speed/dovalid?key=fabricated", ieUA)
	if err != nil {
		t.Fatalf("transport error: %v", err)
	}
	if !strings.Contains(body, "-1") {
		t.Fatalf("unknown key must yield -1-, got %q", body)
	}
	if _, err = DownloadTest(context.Background(), srv, Session{Key: "fabricated"}, 2, time.Second, 0); err == nil {
		t.Fatal("download with fabricated key must fail")
	}
}

func TestEndToEndAgainstMock(t *testing.T) {
	m, err := NewMockNode(32 << 20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	srv := m.Server()
	sess := DefaultSession(500)
	ctx := context.Background()

	// 1. latency
	rtts, err := TCPPing(ctx, srv, 3)
	if err != nil || len(rtts) != 3 {
		t.Fatalf("tcp ping: %v %v", rtts, err)
	}

	// 2. enqueue: empty key → node issues the session uuid
	uuid, _, err := IssueSession(ctx, srv, sess)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	sess.Key = uuid

	// 3. download (multi-thread)
	dl, err := DownloadTest(ctx, srv, sess, 4, 2*time.Second, 0)
	if err != nil {
		t.Fatalf("download: %v (status %d)", err, dl.StatusCode)
	}
	if dl.Bytes == 0 || dl.AvgBps <= 0 {
		t.Fatalf("no download throughput: %+v", dl)
	}

	// 4. ranged probe with the issued uuid
	got, status, err := DownloadProbe(ctx, srv, uuid, 4096)
	if err != nil || got != 4096 || status != 200 {
		t.Fatalf("probe: %d bytes, status %d, err %v", got, status, err)
	}

	// 5. upload (multipart → doAnalsLoad.do)
	ul, err := UploadTest(ctx, srv, sess, 2, 2*time.Second, 128<<10)
	if err != nil {
		t.Fatalf("upload: %v (status %d)", err, ul.StatusCode)
	}
	if ul.Bytes == 0 {
		t.Fatalf("no upload throughput: %+v", ul)
	}
	deadline := time.Now().Add(5 * time.Second)
	for m.Uploaded() < ul.Bytes/2 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if m.Uploaded() < ul.Bytes/2 {
		t.Fatalf("mock received %d bytes but client sent %d", m.Uploaded(), ul.Bytes)
	}

	// 6. dequeue
	Dequeue(ctx, srv, uuid)

	// 7. report line
	st := pingSummary(rtts, 0)
	line := ReportLine(srv, st.Avg, 0, st.Jitter, dl.AvgBps, dl.MaxBps, ul.AvgBps, ul.MaxBps)
	if cols := strings.Split(line, ";"); len(cols) != 12 {
		t.Fatalf("report line schema mismatch: %q", line)
	}
}

func TestPingSummary(t *testing.T) {
	st := pingSummary([]float64{10, 12, 11}, 1)
	if st.Avg != 11 || st.Count != 3 {
		t.Fatalf("unexpected summary: %+v", st)
	}
	if st.Loss != 25 { // 1 err / 4 attempts
		t.Fatalf("loss calc wrong: %v", st.Loss)
	}
}
