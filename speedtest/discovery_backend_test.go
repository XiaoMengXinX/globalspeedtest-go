package speedtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/XiaoMengXinX/globalspeedtest-go/speedtest/caict"
)

const fixtureLocation = `192.0.2.10|["中国","测试省","测试市","测试区","测试运营商"]|1|测试运营商|100000`

func discoveryFixture(t *testing.T) (*Speedtest, *httptest.Server) {
	t.Helper()
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dataServer/getIpLocSP.php":
			fmt.Fprint(w, fixtureLocation)
		case "/dataServer/mobilematch_many.php":
			host, port, _ := net.SplitHostPort(server.Listener.Addr().String())
			json.NewEncoder(w).Encode([]map[string]string{{
				"hostid": "900001", "hostname": "本地测试节点", "hostip": host, "port": port,
				"pname": "测试省", "city": "测试市", "oper": "测试运营商",
			}})
		default:
			fmt.Fprint(w, "ok")
		}
	}))
	t.Cleanup(server.Close)
	return New(WithUserConfig(&UserConfig{ControlURL: server.URL, DeviceID: "TS0000000000000000", PingMode: TCP})), server
}

func TestDiscoveryConcurrentCache(t *testing.T) {
	var locations, registries atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.UserAgent() != controlUserAgent {
			t.Errorf("user agent = %q", r.UserAgent())
		}
		switch r.URL.Path {
		case "/dataServer/getIpLocSP.php":
			locations.Add(1)
			if r.URL.Query().Get("mac") != "TS0000000000000000" || r.URL.Query().Get("tag") != "ios" {
				t.Errorf("location query = %v", r.URL.Query())
			}
			fmt.Fprint(w, fixtureLocation)
		case "/dataServer/mobilematch_many.php":
			registries.Add(1)
			for key, want := range map[string]string{
				"ip": "192.0.2.10", "network": "4", "province": "测试省", "city": "测试市",
				"wifioper": "测试运营商", "mobileoperid": "", "ipv6": "0", "model": "iphone", "pkg": caict.DefaultPkg,
			} {
				if got := r.URL.Query().Get(key); got != want || !r.URL.Query().Has(key) {
					t.Errorf("%s = %q, want %q", key, got, want)
				}
			}
			fmt.Fprint(w, `[{"hostid":900001,"hostname":"本地节点","hostip":"127.0.0.1","port":8080}]`)
		default:
			t.Errorf("unexpected request %s", r.URL)
		}
	}))
	defer server.Close()
	client := New(WithUserConfig(&UserConfig{ControlURL: server.URL, DeviceID: "TS0000000000000000"}), WithDoer(server.Client()))
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				if _, err := client.FetchUserInfo(); err != nil {
					t.Error(err)
				}
			} else if _, err := client.FetchServerByID("900001"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if locations.Load() != 1 || registries.Load() != 1 {
		t.Fatalf("location/registry calls = %d/%d", locations.Load(), registries.Load())
	}
	first, _ := client.FetchServerByID("900001")
	first.Name = "changed"
	second, _ := client.FetchServerByID("900001")
	if second.Name == first.Name {
		t.Fatal("registry returned mutable cached server")
	}
}

func TestDiscoveryNodeValidation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dataServer/getIpLocSP.php" {
			fmt.Fprint(w, fixtureLocation)
			return
		}
		fmt.Fprint(w, `[
			{"hostid":900001,"hostname":"IPv6","hostip":"2001:db8::1","port":8080,"oper":"测试"},
			{"hostid":"900002","hostname":"duplicate address","hostip":"[2001:db8::1]","port":"8080"},
			{"hostid":"900001","hostname":"duplicate ID","hostip":"192.0.2.1","port":"8080"},
			{"hostname":"missing ID","hostip":"192.0.2.1","port":8080},
			{"hostid":900003,"hostname":"bad port","hostip":"192.0.2.1","port":65536},
			{"hostid":900004,"hostname":"bad host","hostip":"http://example.test/path","port":8080},
			{"hostid":900005,"hostname":"fractional port","hostip":"192.0.2.1","port":80.5},
			{"hostid":900006,"hostip":"192.0.2.1","port":80},
			{"hostid":null,"hostname":"null ID","hostip":"192.0.2.1","port":80}
		]`)
	}))
	defer server.Close()
	client := New(WithUserConfig(&UserConfig{ControlURL: server.URL}))
	servers, err := client.fetchRegistry(context.Background())
	if err != nil || len(servers) != 1 {
		t.Fatalf("servers = %+v, %v", servers, err)
	}
	s := servers[0]
	if s.ID != "900001" || s.Host != "[2001:db8::1]:8080" || s.URL != "http://[2001:db8::1]:8080/speed/doAnalsLoad.do" {
		t.Fatalf("server = %+v", s)
	}
	if s.Distance != -1 || strings.Contains(s.String(), "km") {
		t.Fatalf("distance = %s", s.String())
	}
}

func TestDiscoveryFiltersBeforePing(t *testing.T) {
	var pings atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dataServer/getIpLocSP.php":
			fmt.Fprint(w, fixtureLocation)
		case "/dataServer/mobilematch_many.php":
			host, port, _ := net.SplitHostPort(server.Listener.Addr().String())
			json.NewEncoder(w).Encode([]map[string]string{
				{"hostid": "900001", "hostname": "北京节点", "hostip": host, "port": port, "pname": "北京", "city": "北京", "oper": "测试"},
				{"hostid": "900002", "hostname": "上海节点", "hostip": "192.0.2.2", "port": port, "pname": "上海", "city": "上海", "oper": "其他"},
			})
		default:
			pings.Add(1)
		}
	}))
	defer server.Close()
	client := New(WithUserConfig(&UserConfig{ControlURL: server.URL, CityFlag: "beijing", Operator: "测试", Keyword: "北京"}))
	servers, err := client.FetchServers()
	if err != nil || len(servers) != 1 || servers[0].ID != "900001" || pings.Load() != 1 {
		t.Fatalf("servers = %+v, err = %v, pings = %d", servers, err, pings.Load())
	}
	serverByID, err := client.FetchServerByID("900002")
	if err != nil || serverByID.ID != "900002" {
		t.Fatalf("lookup outside filter = %+v, %v", serverByID, err)
	}
	client.config.Keyword = "no matching node"
	_, err = client.FetchServers()
	if !errors.Is(err, ErrServerNotFound) || pings.Load() != 1 {
		t.Fatalf("empty filter: %v, pings %d", err, pings.Load())
	}
}

func TestDiscoverySearchFallsBackToEmbedded(t *testing.T) {
	client := New(WithUserConfig(&UserConfig{Keyword: "北京"}))
	embedded, err := client.embeddedServers()
	if err != nil {
		t.Fatal(err)
	}
	live := Servers{{ID: "900001", Name: "本地测试节点", Province: "测试省", City: "测试市", Operator: "测试运营商"}}
	if got := client.filterNodes(live, "", ""); len(got) != 0 {
		t.Fatalf("keyword 北京 matched the local feed: %+v", got)
	}
	got := client.filterNodes(embedded, "", "")
	if len(got) == 0 {
		t.Fatalf("keyword 北京 missing from embedded table")
	}
	for _, s := range got {
		if !strings.Contains(s.ID+s.Name+s.Province+s.City+s.Operator+s.Host, "北京") {
			t.Fatalf("filter returned non-matching node: %+v", s)
		}
	}
	if !client.hasSearchCriteria("", "") {
		t.Fatal("keyword should count as a search criterion")
	}
	plain := New(WithUserConfig(&UserConfig{}))
	if plain.hasSearchCriteria("", "") {
		t.Fatal("no criteria should not trigger fallback")
	}
}

func TestDiscoveryFallback(t *testing.T) {
	for _, response := range []string{"[]", "invalid JSON", `[{"hostid":1,"hostname":"invalid","hostip":"bad host","port":0}]`} {
		t.Run(response, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/dataServer/getIpLocSP.php" {
					fmt.Fprint(w, fixtureLocation)
					return
				}
				fmt.Fprint(w, response)
			}))
			defer server.Close()
			client := New(WithUserConfig(&UserConfig{ControlURL: server.URL}))
			servers, err := client.fetchRegistry(context.Background())
			if err != nil || len(servers) == 0 {
				t.Fatalf("fallback = %d, %v", len(servers), err)
			}
		})
	}
	client, _ := discoveryFixture(t)
	live, err := client.fetchRegistry(context.Background())
	if err != nil || len(live) != 1 {
		t.Fatalf("live registry = %d, %v", len(live), err)
	}
	embedded, err := caict.EmbeddedServerList()
	if err != nil {
		t.Fatal(err)
	}
	id := embedded.Servers[len(embedded.Servers)-1].HostID
	if server, err := client.FetchServerByID(id); err != nil || server.ID != id {
		t.Fatalf("embedded lookup = %+v, %v", server, err)
	}
}

func TestDiscoveryCancellation(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	client := New(WithUserConfig(&UserConfig{ControlURL: server.URL}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := client.FetchServerListContext(ctx); done <- err }()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
	if client.registry != nil || client.userLookup != nil {
		t.Fatal("canceled lookup cached")
	}
}

func TestControlResponseValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"status", http.StatusBadGateway, fixtureLocation},
		{"oversize", http.StatusOK, strings.Repeat("x", (4<<20)+1)},
		{"invalid IP", http.StatusOK, `invalid|["中国","省","市"]|1|测试`},
		{"invalid region", http.StatusOK, `192.0.2.1|invalid|1|测试`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status); fmt.Fprint(w, tc.body) }))
			defer server.Close()
			client := New(WithUserConfig(&UserConfig{ControlURL: server.URL}))
			if user, err := client.FetchUserInfo(); err == nil || user != nil {
				t.Fatalf("invalid response accepted: %+v, %v", user, err)
			}
		})
	}
}

func TestCustomNodeAddresses(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"127.0.0.1:8080", "http://127.0.0.1:8080/speed/doAnalsLoad.do"},
		{"[2001:db8::1]:8080", "http://[2001:db8::1]:8080/speed/doAnalsLoad.do"},
		{"https://example.test/old?x=y#fragment", "https://example.test:443/speed/doAnalsLoad.do"},
	} {
		server, err := CustomServer(tc.input)
		if err != nil || server.URL != tc.want {
			t.Errorf("%s = %+v, %v", tc.input, server, err)
		}
	}
	for _, input := range []string{"", "ftp://example.test", "http://user:pass@example.test", "http://example.test:0", "http://example.test:65536", "http:///missing"} {
		if server, err := CustomServer(input); err == nil {
			t.Errorf("accepted %q: %+v", input, server)
		}
	}
}
