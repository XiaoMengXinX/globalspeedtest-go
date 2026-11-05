package speedtest

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func BenchmarkLogSpeed(b *testing.B) {
	s := New()
	config := &UserConfig{
		UserAgent: DefaultUserAgent,
		Debug:     false,
	}
	WithUserConfig(config)(s)
	for i := 0; i < b.N; i++ {
		dbg.Printf("hello %s\n", "s20080123") // ~1ns/op
	}
}

func TestClientDefaults(t *testing.T) {
	transport := http.DefaultClient.Transport
	first, second := New(), New()
	if first.doer == http.DefaultClient || first.doer == second.doer || http.DefaultClient.Transport != transport {
		t.Fatal("clients share or mutate the default HTTP client")
	}
	if first.config.ControlURL != "https://dlcv2.cnspeedtest.cn:8443" || first.config.Bandwidth != 500 || first.config.DeviceID == "" || first.config.DeviceID == second.config.DeviceID {
		t.Fatalf("defaults = %+v", first.config)
	}
	if !first.config.T.DisableCompression {
		t.Fatal("compression enabled")
	}
}

func TestProtocolUserAgent(t *testing.T) {
	var seen string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.UserAgent()
	}))
	defer server.Close()

	t.Run("protocol header preserved", func(t *testing.T) {
		client := New()
		req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
		req.Header.Set("User-Agent", "node-protocol")
		resp, err := client.doer.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if seen != "node-protocol" {
			t.Fatalf("user agent = %q", seen)
		}
		if req.UserAgent() != "node-protocol" {
			t.Fatal("request mutated")
		}
	})

	t.Run("explicit override replaces protocol header", func(t *testing.T) {
		client := New(WithUserConfig(&UserConfig{UserAgent: "configured"}))
		req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
		req.Header.Set("User-Agent", "node-protocol")
		resp, err := client.doer.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if seen != "configured" {
			t.Fatalf("user agent = %q", seen)
		}
	})
}

func TestNew(t *testing.T) {
	t.Run("DefaultDoer", func(t *testing.T) {
		c := New()

		if c.doer == nil {
			t.Error("doer is nil by")
		}
	})

	t.Run("CustomDoer", func(t *testing.T) {
		doer := &http.Client{}

		c := New(WithDoer(doer))
		if c.doer != doer {
			t.Error("doer is not the same")
		}
	})
}

func TestUserAgent(t *testing.T) {
	testServer := func(expectedUserAgent string) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.UserAgent() == "" {
				t.Error("did not receive User-Agent header")
			} else if r.UserAgent() != expectedUserAgent {
				t.Errorf("incorrect User-Agent header: %s, expected: %s", r.UserAgent(), expectedUserAgent)
			}
		}))
		t.Cleanup(server.Close)
		return server
	}

	t.Run("DefaultUserAgent", func(t *testing.T) {
		c := New(WithUserConfig(&UserConfig{UserAgent: DefaultUserAgent}))
		s := testServer(DefaultUserAgent)
		resp, err := c.doer.Get(s.URL)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	})

	t.Run("CustomUserAgent", func(t *testing.T) {
		testAgent := "1234"
		s := testServer(testAgent)
		c := New(WithUserConfig(&UserConfig{UserAgent: testAgent}))
		resp, err := c.doer.Get(s.URL)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	})

	t.Run("CustomUserAgentAndDoer", func(t *testing.T) {
		testAgent := "4321"
		doer := &http.Client{}
		s := testServer(testAgent)
		c := New(WithDoer(doer), WithUserConfig(&UserConfig{UserAgent: testAgent}))
		if c.doer != doer {
			t.Error("doer is not the same")
		}
		resp, err := c.doer.Get(s.URL)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	})
}
