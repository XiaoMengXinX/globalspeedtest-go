package caict

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestMockRequiresTokenAndRevokesReleasedKey(t *testing.T) {
	m, err := NewMockNode(4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	body, err := httpGetString(ctx, "http://"+m.Addr()+"/speed/dovalid?key=&flag=true&bandwidth=500&model=iphone&imei=TS0123456789ABCDEF&time=1700000000&token=wrong", ieUA)
	if err != nil {
		t.Fatal(err)
	}
	if body != "-1-" {
		t.Fatalf("bad token accepted: %q", body)
	}
	session := DefaultSession(500)
	for i := 0; i < 2; i++ {
		key, _, err := IssueSession(ctx, m.Server(), session)
		if err != nil {
			t.Fatal(err)
		}
		if len(key) != 36 {
			t.Fatalf("malformed mock uuid %q", key)
		}
		Dequeue(ctx, m.Server(), key)
		_, status, err := DownloadProbe(ctx, m.Server(), key, 128)
		if err == nil || status != http.StatusForbidden {
			t.Fatalf("released key still accepted: status=%d err=%v", status, err)
		}
	}
}
