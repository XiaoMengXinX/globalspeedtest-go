package speedtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XiaoMengXinX/globalspeedtest-go/speedtest/caict"
	"github.com/XiaoMengXinX/globalspeedtest-go/speedtest/transport"
)

const dataPlanePath = "/speed/doAnalsLoad.do"

type registryLookup struct {
	done    chan struct{}
	servers Servers
	err     error
}

var (
	ErrServerNotFound = errors.New("no server available or found")
)

// Server information
type Server struct {
	URL          string          `xml:"url,attr" json:"url"`
	Lat          string          `xml:"lat,attr" json:"lat"`
	Lon          string          `xml:"lon,attr" json:"lon"`
	Name         string          `xml:"name,attr" json:"name"`
	Country      string          `xml:"country,attr" json:"country"`
	Sponsor      string          `xml:"sponsor,attr" json:"sponsor"`
	ID           string          `xml:"id,attr" json:"id"`
	Host         string          `xml:"host,attr" json:"host"`
	Distance     float64         `json:"distance"`
	Latency      time.Duration   `json:"latency"`
	MaxLatency   time.Duration   `json:"max_latency"`
	MinLatency   time.Duration   `json:"min_latency"`
	Jitter       time.Duration   `json:"jitter"`
	DLSpeed      ByteRate        `json:"dl_speed"`
	ULSpeed      ByteRate        `json:"ul_speed"`
	TestDuration TestDuration    `json:"test_duration"`
	CC           string          `json:"cc"`
	PacketLoss   transport.PLoss `json:"packet_loss"`

	Province string `json:"province,omitempty"`
	City     string `json:"city,omitempty"`
	Operator string `json:"operator,omitempty"`

	Context *Speedtest `json:"-"`
}

type TestDuration struct {
	Ping     *time.Duration `json:"ping"`
	Download *time.Duration `json:"download"`
	Upload   *time.Duration `json:"upload"`
	Total    *time.Duration `json:"total"`
}

// CustomServer use defaultClient, given a URL string, return a new Server object, with as much
// filled in as we can
func CustomServer(host string) (*Server, error) {
	return defaultClient.CustomServer(host)
}

// CustomServer given a URL string, return a new Server object, with as much
// filled in as we can
func (s *Speedtest) CustomServer(host string) (*Server, error) {
	if !strings.Contains(host, "://") {
		host = "http://" + host
	}
	u, err := url.Parse(host)
	if err != nil {
		return nil, err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return nil, errors.New("invalid node URL")
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "https" {
			port = "443"
		}
	}
	if !validNodeHost(u.Hostname()) || !validNodePort(port) {
		return nil, errors.New("invalid node address")
	}
	u.Host = net.JoinHostPort(u.Hostname(), port)
	u.Path, u.RawPath, u.RawQuery, u.Fragment = dataPlanePath, "", "", ""
	return &Server{
		ID: "Custom", URL: u.String(), Name: u.Host, Host: u.Host,
		Sponsor: "?", Distance: -1, Context: s,
	}, nil
}

// ServerList contains servers and optional client information.
type ServerList struct {
	Servers []*Server `xml:"servers>server"`
	Users   []User    `xml:"client"`
}

// Servers for sorting servers.
type Servers []*Server

// ByDistance for sorting servers.
type ByDistance struct {
	Servers
}

func (servers Servers) Available() *Servers {
	retServer := Servers{}
	for _, server := range servers {
		if server.Latency != PingTimeout {
			retServer = append(retServer, server)
		}
	}
	for i := 0; i < len(retServer)-1; i++ {
		for j := 0; j < len(retServer)-i-1; j++ {
			if retServer[j].Latency > retServer[j+1].Latency {
				retServer[j], retServer[j+1] = retServer[j+1], retServer[j]
			}
		}
	}
	return &retServer
}

// Len finds length of servers. For sorting servers.
func (servers Servers) Len() int {
	return len(servers)
}

// Swap swaps i-th and j-th. For sorting servers.
func (servers Servers) Swap(i, j int) {
	servers[i], servers[j] = servers[j], servers[i]
}

// Filter filter by filterFunc
func (servers Servers) Filter(filterFunc func(server *Server) bool) Servers {
	var retServers Servers
	for i := range servers {
		if filterFunc(servers[i]) {
			retServers = append(retServers, servers[i])
		}
	}
	return retServers
}

// CC filter by Country Code
func (servers Servers) CC(cc []string) Servers {
	var upperCC []string
	for i := range cc {
		upperCC = append(upperCC, strings.ToUpper(cc[i]))
	}
	return servers.Filter(func(server *Server) bool {
		for _, code := range upperCC {
			if code == server.CC {
				return true
			}
		}
		return false
	})
}

// Hosts return hosts of servers
func (servers Servers) Hosts() []string {
	var retServer []string
	for _, server := range servers {
		retServer = append(retServer, server.Host)
	}
	return retServer
}

// Less compares the distance. For sorting servers.
func (b ByDistance) Less(i, j int) bool {
	if b.Servers[i].Distance < 0 {
		return false
	}
	return b.Servers[j].Distance < 0 || b.Servers[i].Distance < b.Servers[j].Distance
}

// FetchServerByID retrieves a server by given serverID.
func (s *Speedtest) FetchServerByID(serverID string) (*Server, error) {
	return s.FetchServerByIDContext(context.Background(), serverID)
}

// FetchServerByID retrieves a server by given serverID.
func FetchServerByID(serverID string) (*Server, error) {
	return defaultClient.FetchServerByID(serverID)
}

// FetchServerByIDContext retrieves a server by given serverID, observing the given context.
func (s *Speedtest) FetchServerByIDContext(ctx context.Context, serverID string) (*Server, error) {
	servers, err := s.fetchRegistry(ctx)
	if err != nil {
		return nil, err
	}
	for _, server := range servers {
		if server.ID == serverID {
			copy := *server
			return &copy, nil
		}
	}
	servers, err = s.embeddedServers()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, server := range servers {
		if server.ID == serverID {
			return server, nil
		}
	}
	return nil, ErrServerNotFound
}

// FetchServers retrieves a list of available servers
func (s *Speedtest) FetchServers() (Servers, error) {
	return s.FetchServerListContext(context.Background())
}

// FetchServers retrieves a list of available servers
func FetchServers() (Servers, error) {
	return defaultClient.FetchServers()
}

// FetchServerListContext retrieves a list of available servers, observing the given context.
func (s *Speedtest) FetchServerListContext(ctx context.Context) (Servers, error) {
	registry, err := s.fetchRegistry(ctx)
	if err != nil {
		return nil, err
	}
	city, province := s.matchLocation()
	servers := s.filterNodes(registry, city, province)
	if len(servers) == 0 && s.hasSearchCriteria(city, province) {
		embedded, eerr := s.embeddedServers()
		if eerr != nil {
			return nil, eerr
		}
		servers = s.filterNodes(embedded, city, province)
	}
	if len(servers) == 0 {
		return servers, ErrServerNotFound
	}
	user, _ := s.FetchUserInfoContext(ctx)
	score := func(server *Server) int {
		n := 0
		if user != nil {
			if server.Province == user.Province {
				n += 1
			}
			if server.Operator == user.Operator {
				n += 2
			}
			if server.City == user.City {
				n += 4
			}
		}
		return n
	}
	sort.SliceStable(servers, func(i, j int) bool { return score(servers[i]) > score(servers[j]) })
	if len(servers) > 20 {
		servers = servers[:20]
	}
	pCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, server := range servers {
		wg.Add(1)
		go func(gs *Server) {
			defer wg.Done()
			var latency []int64
			var err error
			switch s.config.PingMode {
			case TCP:
				latency, err = gs.TCPPing(pCtx, 1, time.Millisecond, nil)
			case ICMP:
				latency, err = gs.ICMPPing(pCtx, 4*time.Second, 1, time.Millisecond, nil)
			default:
				latency, err = gs.HTTPPing(pCtx, 1, time.Millisecond, nil)
			}
			gs.Latency = PingTimeout
			if err == nil && len(latency) > 0 {
				gs.Latency = time.Duration(latency[0])
			}
		}(server)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return servers, nil
}

// hasSearchCriteria reports whether the user asked for specific nodes rather
// than location-based recommendations. Such queries fall back to the embedded
// nationwide table when the live feed, which is narrowed to the caller's
// region, cannot satisfy them.
func (s *Speedtest) hasSearchCriteria(city, province string) bool {
	return s.config.Keyword != "" || province != "" || city != "" || s.config.Operator != ""
}

func (s *Speedtest) filterNodes(nodes Servers, city, province string) Servers {
	var out Servers
	for _, node := range nodes {
		if s.config.Operator != "" && !strings.Contains(node.Operator, s.config.Operator) {
			continue
		}
		if province != "" && !strings.Contains(node.Province, province) {
			continue
		}
		if city != "" && !strings.Contains(strings.ToLower(node.City+node.Name), strings.ToLower(city)) {
			continue
		}
		if keyword := strings.ToLower(s.config.Keyword); keyword != "" &&
			!strings.Contains(strings.ToLower(node.ID+" "+node.Name+" "+node.Province+" "+node.City+" "+node.Operator+" "+node.Host), keyword) {
			continue
		}
		copy := *node
		out = append(out, &copy)
	}
	return out
}

func validNodePort(port string) bool {
	n, err := strconv.Atoi(port)
	return err == nil && n > 0 && n <= 65535
}

func validNodeHost(host string) bool {
	if net.ParseIP(host) != nil {
		return true
	}
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if r != '-' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
				return false
			}
		}
	}
	return true
}

func (s *Speedtest) nodeServer(node caict.Server) *Server {
	host := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(node.HostIP), "["), "]")
	if node.HostID == "" || node.HostName == "" || !validNodeHost(host) || !validNodePort(node.Port) {
		return nil
	}
	addr := net.JoinHostPort(host, node.Port)
	return &Server{
		ID: node.HostID, Name: node.HostName, Host: addr,
		URL:     (&url.URL{Scheme: "http", Host: addr, Path: dataPlanePath}).String(),
		Country: "中国", CC: "CN", Sponsor: node.Oper, Operator: node.Oper,
		Province: node.PName, City: node.Location, Distance: -1, Context: s,
	}
}

func (s *Speedtest) embeddedServers() (Servers, error) {
	list, err := caict.EmbeddedServerList()
	if err != nil {
		return nil, err
	}
	var servers Servers
	for _, node := range list.Servers {
		if server := s.nodeServer(node); server != nil {
			servers = append(servers, server)
		}
	}
	return dedupeServers(servers), nil
}

func dedupeServers(servers Servers) Servers {
	ids, hosts := make(map[string]bool), make(map[string]bool)
	out := make(Servers, 0, len(servers))
	for _, server := range servers {
		if ids[server.ID] || hosts[server.Host] {
			continue
		}
		ids[server.ID], hosts[server.Host] = true, true
		out = append(out, server)
	}
	return out
}

func (s *Speedtest) matchLocation() (city, province string) {
	province = s.config.Province
	city = s.config.CityFlag
	if s.config.Location != nil && city == "" {
		city = s.config.Location.Name
		if strings.HasPrefix(city, "Custom-") {
			city = ""
		}
	}
	switch strings.ToLower(city) {
	case "beijing":
		city = "北京"
	case "shanghai":
		city = "上海"
	case "hongkong":
		city = "香港"
	case "urumqi":
		city = "乌鲁木齐"
		if province == "" {
			province = "新疆"
		}
	}
	if province == "" && (city == "北京" || city == "上海" || city == "香港") {
		province = city
	}
	return city, province
}

func (s *Speedtest) liveServers(ctx context.Context) (Servers, error) {
	user, _ := s.FetchUserInfoContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v := url.Values{
		"ip": {""}, "network": {"4"}, "province": {""}, "city": {""},
		"wifioper": {""}, "mobileoperid": {""}, "ipv6": {"0"},
		"model": {"iphone"}, "pkg": {caict.DefaultPkg},
	}
	if user != nil {
		v.Set("ip", user.IP)
		v.Set("province", user.Province)
		v.Set("city", user.City)
		v.Set("wifioper", user.Operator)
		if ip := net.ParseIP(user.IP); ip != nil && ip.To4() == nil {
			v.Set("ipv6", "1")
		}
	}
	city, province := s.matchLocation()
	if city != "" {
		v.Set("city", city)
	}
	if province != "" {
		v.Set("province", province)
	}
	if s.config.Operator != "" {
		v.Set("wifioper", s.config.Operator)
	}
	body, err := s.controlGet(ctx, "/dataServer/mobilematch_many.php", v)
	if err != nil {
		return nil, err
	}
	var nodes []map[string]json.RawMessage
	if err := json.Unmarshal(body, &nodes); err != nil {
		return nil, err
	}
	var servers Servers
	for _, node := range nodes {
		text := func(key string) string {
			var str string
			if json.Unmarshal(node[key], &str) == nil {
				return strings.TrimSpace(str)
			}
			var n json.Number
			if json.Unmarshal(node[key], &n) == nil {
				return n.String()
			}
			return ""
		}
		oper := text("oper")
		if oper == "" {
			oper = v.Get("wifioper")
		}
		city := text("city")
		if city == "" {
			city = text("location")
		}
		server := s.nodeServer(caict.Server{
			HostID: text("hostid"), HostName: text("hostname"), HostIP: text("hostip"),
			Port: text("port"), PName: text("pname"), Location: city, Oper: oper,
		})
		if server != nil {
			servers = append(servers, server)
		}
	}
	return dedupeServers(servers), nil
}

func (s *Speedtest) fetchRegistry(ctx context.Context) (Servers, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.userMu.Lock()
	lookup := s.registry
	if lookup != nil {
		s.userMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-lookup.done:
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return lookup.servers, lookup.err
		}
	}
	lookup = &registryLookup{done: make(chan struct{})}
	s.registry = lookup
	s.userMu.Unlock()
	servers, err := s.liveServers(ctx)
	if ctx.Err() != nil {
		err = ctx.Err()
	} else if err != nil || len(servers) == 0 {
		servers, err = s.embeddedServers()
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	s.userMu.Lock()
	lookup.servers, lookup.err = servers, err
	if s.registry == lookup && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		s.registry = nil
	}
	close(lookup.done)
	s.userMu.Unlock()
	return servers, err
}

// FetchServerListContext retrieves a list of available servers, observing the given context.
func FetchServerListContext(ctx context.Context) (Servers, error) {
	return defaultClient.FetchServerListContext(ctx)
}

func distance(lat1 float64, lon1 float64, lat2 float64, lon2 float64) float64 {
	radius := 6378.137

	phi1 := lat1 * math.Pi / 180.0
	phi2 := lat2 * math.Pi / 180.0

	deltaPhiHalf := (lat1 - lat2) * math.Pi / 360.0
	deltaLambdaHalf := (lon1 - lon2) * math.Pi / 360.0
	sinePhiHalf2 := math.Sin(deltaPhiHalf)*math.Sin(deltaPhiHalf) + math.Cos(phi1)*math.Cos(phi2)*math.Sin(deltaLambdaHalf)*math.Sin(deltaLambdaHalf) // phi half-angle sine ^ 2
	delta := 2 * math.Atan2(math.Sqrt(sinePhiHalf2), math.Sqrt(1-sinePhiHalf2))                                                                       // 2 arc sine
	return radius * delta                                                                                                                             // r * delta
}

// FindServer finds server by serverID in given server list.
// If the id is not found in the given list, return the server with the lowest latency.
func (servers Servers) FindServer(serverID []int) (Servers, error) {
	retServer := Servers{}

	if len(servers) <= 0 {
		return retServer, ErrServerNotFound
	}

	for _, sid := range serverID {
		for _, s := range servers {
			id, _ := strconv.Atoi(s.ID)
			if sid == id {
				retServer = append(retServer, s)
				break
			}
		}
	}

	if len(retServer) == 0 {
		// choose the lowest latency server
		var minLatency int64 = math.MaxInt64
		var minServerIndex int
		for index, server := range servers {
			if server.Latency <= 0 {
				continue
			}
			if minLatency > server.Latency.Milliseconds() {
				minLatency = server.Latency.Milliseconds()
				minServerIndex = index
			}
		}
		retServer = append(retServer, servers[minServerIndex])
	}
	return retServer, nil
}

// String representation of ServerList
func (servers ServerList) String() string {
	slr := ""
	for _, server := range servers.Servers {
		slr += server.String()
	}
	return slr
}

// String representation of Servers
func (servers Servers) String() string {
	slr := ""
	for _, server := range servers {
		slr += server.String()
	}
	return slr
}

// String representation of Server
func (s *Server) String() string {
	if s.Sponsor == "?" {
		return fmt.Sprintf("[%4s] %s", s.ID, s.Name)
	}
	distance := ""
	if s.Distance >= 0 {
		distance = fmt.Sprintf("%.2fkm ", s.Distance)
	}
	if len(s.Country) == 0 {
		return fmt.Sprintf("[%4s] %s%s by %s", s.ID, distance, s.Name, s.Sponsor)
	}
	return fmt.Sprintf("[%4s] %s%s (%s) by %s", s.ID, distance, s.Name, s.Country, s.Sponsor)
}

// CheckResultValid checks that results are logical given UL and DL speeds
func (s *Server) CheckResultValid() bool {
	return !(s.DLSpeed*100 < s.ULSpeed) || !(s.DLSpeed > s.ULSpeed*100)
}

func (s *Server) testDurationTotalCount() {
	total := s.getNotNullValue(s.TestDuration.Ping) +
		s.getNotNullValue(s.TestDuration.Download) +
		s.getNotNullValue(s.TestDuration.Upload)

	s.TestDuration.Total = &total
}

func (s *Server) getNotNullValue(time *time.Duration) time.Duration {
	if time == nil {
		return 0
	}
	return *time
}
