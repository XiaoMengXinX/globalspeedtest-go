package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/XiaoMengXinX/globalspeedtest-go/speedtest"
	"github.com/alecthomas/kingpin/v2"
)

var (
	showList      = kingpin.Flag("list", "List CAICT servers.").Short('l').Bool()
	serverIds     = kingpin.Flag("server", "Select server IDs.").Short('s').Ints()
	customURL     = kingpin.Flag("custom-url", "Use a CAICT node URL or host:port.").String()
	savingMode    = kingpin.Flag("saving-mode", "Use one connection per test.").Bool()
	jsonOutput    = kingpin.Flag("json", "Output results as JSON.").Bool()
	jsonlOutput   = kingpin.Flag("jsonl", "Output one JSON object per server.").Bool()
	unixOutput    = kingpin.Flag("unix", "Print completed tasks as plain text.").Bool()
	location      = kingpin.Flag("location", "Set a location (lat,lon).").String()
	city          = kingpin.Flag("city", "Select a city.").String()
	showCityList  = kingpin.Flag("city-list", "List predefined cities.").Bool()
	province      = kingpin.Flag("province", "Select a province.").String()
	operator      = kingpin.Flag("operator", "Select an operator: 电信, 联通, 移动, 教育网, 广电网.").String()
	proxy         = kingpin.Flag("proxy", "Use an HTTP, HTTPS or SOCKS5 proxy.").String()
	source        = kingpin.Flag("source", "Bind to a source IP or network interface.").String()
	dnsBindSource = kingpin.Flag("dns-bind-source", "Bind DNS requests to the source address.").Bool()
	multi         = kingpin.Flag("multi", "Measure using multiple servers.").Short('m').Bool()
	thread        = kingpin.Flag("thread", "Maximum concurrent connections.").Short('t').Int()
	search        = kingpin.Flag("search", "Search server names.").String()
	userAgent     = kingpin.Flag("ua", "Override the User-Agent on every request, replacing the protocol defaults.").String()
	noDownload    = kingpin.Flag("no-download", "Skip the download test.").Bool()
	noUpload      = kingpin.Flag("no-upload", "Skip the upload test.").Bool()
	pingMode      = kingpin.Flag("ping-mode", "Latency probe: icmp, tcp or http.").Default("http").Enum("icmp", "tcp", "http")
	protocol      = kingpin.Flag("protocol", "Measurement protocol.").Short('p').Default("http").Enum("http")
	unit          = kingpin.Flag("unit", "Rate units: decimal-bits, decimal-bytes, binary-bits or binary-bytes.").Short('u').String()
	countryCode   = kingpin.Flag("filter-cc", "Filter by country code.").Strings()
	maskISP       = kingpin.Flag("mask-isp", "Mask the displayed IP address and coordinates.").Bool()
	debug         = kingpin.Flag("debug", "Enable debug logging.").Short('d').Bool()
	duration      = kingpin.Flag("duration", "Maximum measurement time per direction.").Default("10s").Duration()
	bandwidth     = kingpin.Flag("bandwidth", "Bandwidth hint for session requests, in Mbps.").Default("500").Int()
	deviceID      = kingpin.Flag("device", "Use a fixed device identifier.").String()
	controlURL    = kingpin.Flag("control-url", "CAICT control API base URL.").Default("https://dlcv2.cnspeedtest.cn:8443").String()
	fallback      = kingpin.Flag("fallback", "Servers to try by latency when auto-selecting before giving up.").Default("3").Int()
)

var (
	commit = "dev"
	date   = "unknown"
)

func main() {
	kingpin.Version(fmt.Sprintf("globalspeedtest-go v%s git-%s built at %s", speedtest.Version(), commit, date))
	kingpin.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runSpeedtest(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runSpeedtest(ctx context.Context) error {
	if *duration <= 0 || *bandwidth <= 0 || *thread < 0 || *fallback < 1 {
		return errors.New("duration, bandwidth and fallback must be positive; thread must be nonnegative")
	}
	if *jsonOutput && *jsonlOutput {
		return errors.New("choose either --json or --jsonl")
	}
	if *showCityList {
		speedtest.PrintCityList()
		return nil
	}
	structured := *jsonOutput || *jsonlOutput
	AppInfo()
	speedtest.SetUnit(parseUnit(*unit))
	log.SetOutput(io.Discard)

	client := speedtest.New(speedtest.WithUserConfig(&speedtest.UserConfig{
		UserAgent: *userAgent, Proxy: *proxy, Source: *source,
		DnsBindSource: *dnsBindSource, Debug: *debug,
		PingMode: parseProto(*pingMode), TestMode: parseTestMode(*protocol),
		SavingMode: *savingMode, MaxConnections: *thread,
		CityFlag: *city, LocationFlag: *location, Keyword: *search,
		Province: *province, Operator: *operator,
		ControlURL: *controlURL, DeviceID: *deviceID, Bandwidth: *bandwidth,
	}))
	client.SetCaptureTime(*duration)
	tm := InitTaskManager(structured, !structured && (*unixOutput || *savingMode))
	defer tm.Stop()

	if err := runTask(tm, "Retrieving User Information", func(task *Task) error {
		u, err := client.FetchUserInfoContext(ctx)
		if err != nil {
			return err
		}
		task.Printf("ISP: %s", formatUserInfo(u, *maskISP))
		return nil
	}); err != nil {
		return err
	}

	var servers, targets speedtest.Servers
	if err := runTask(tm, "Retrieving Servers", func(task *Task) error {
		switch {
		case *customURL != "":
			s, err := client.CustomServer(*customURL)
			if err != nil {
				return err
			}
			servers = speedtest.Servers{s}
			targets = servers
		case len(*serverIds) > 0:
			seen := make(map[int]bool)
			for _, id := range *serverIds {
				if seen[id] {
					continue
				}
				seen[id] = true
				s, err := client.FetchServerByIDContext(ctx, strconv.Itoa(id))
				if err != nil {
					return fmt.Errorf("server %d: %w", id, err)
				}
				targets = append(targets, s)
			}
			servers = targets
		default:
			var err error
			servers, err = client.FetchServerListContext(ctx)
			if err != nil {
				return err
			}
			if len(*countryCode) > 0 {
				codes := append([]string(nil), (*countryCode)...)
				if containsString(codes, "auto") {
					codes = append(codes, client.User.Country)
				}
				servers = servers.CC(codes)
			}
			if !*showList {
				targets, err = servers.FindServer(nil)
				if err != nil {
					return err
				}
			}
		}
		if len(servers) == 0 {
			return errors.New("no matching servers")
		}
		task.Printf("Found %d CAICT Server(s)", len(servers))
		return nil
	}); err != nil {
		return err
	}

	if *showList {
		tm.Stop()
		if structured {
			return printResults(client, servers)
		}
		showServerList(servers)
		return nil
	}

	autoSelect := *customURL == "" && len(*serverIds) == 0 && !*multi
	if autoSelect {
		tested, err := measureWithFallback(ctx, tm, client, servers)
		if err != nil {
			return err
		}
		targets = tested
	} else {
		for _, server := range targets {
			if err := measureServer(ctx, tm, client, server, servers); err != nil {
				return err
			}
			client.Reset()
		}
	}
	tm.Stop()
	if structured {
		return printResults(client, targets)
	}
	return nil
}

// measureWithFallback measures the lowest-latency server and, when a server
// fails before it has delivered any data, advances to the next candidate by
// latency. It returns the server that produced a result.
func measureWithFallback(ctx context.Context, tm *TaskManager, client *speedtest.Speedtest, servers speedtest.Servers) (speedtest.Servers, error) {
	candidates := *servers.Available()
	if len(candidates) == 0 {
		return nil, errors.New("no reachable servers")
	}
	if *fallback < len(candidates) {
		candidates = candidates[:*fallback]
	}
	var lastErr error
	for i, server := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := measureServer(ctx, tm, client, server, servers); err != nil {
			// Once the server has delivered data it works, and a later failure,
			// such as a weak uplink, would follow the measurement to any server.
			if client.GetTotalDownload() > 0 || client.GetTotalUpload() > 0 {
				return nil, err
			}
			lastErr = err
			client.Reset()
			if i < len(candidates)-1 {
				tm.Println("Switching to the next server")
			}
			continue
		}
		return speedtest.Servers{server}, nil
	}
	return nil, lastErr
}

func runTask(tm *TaskManager, title string, fn func(*Task) error) error {
	var err error
	tm.Run(title, func(task *Task) {
		err = fn(task)
		if err != nil {
			task.Printf("%s: %v", title, err)
			task.handle.Error()
			return
		}
		task.Complete()
	})
	if err != nil {
		return fmt.Errorf("%s: %w", strings.ToLower(title), err)
	}
	return nil
}

func measureServer(ctx context.Context, tm *TaskManager, client *speedtest.Speedtest, server *speedtest.Server, servers speedtest.Servers) error {
	tm.BlankLine()
	tm.Println("Test Server: " + server.String())
	if err := runTask(tm, "Latency", func(task *Task) error {
		if err := server.PingTestContext(ctx, func(latency time.Duration) {
			task.Updatef("Latency: %v", latency)
		}); err != nil {
			return err
		}
		task.Printf("Latency: %v Jitter: %v Min: %v Max: %v", server.Latency, server.Jitter, server.MinLatency, server.MaxLatency)
		return nil
	}); err != nil {
		return err
	}

	echo := newAccompanyEcho(server, 500*time.Millisecond)
	defer echo.Stop()
	if !*noDownload {
		if err := runTask(tm, "Download", func(task *Task) error {
			echo.RunContext(ctx)
			defer echo.Stop()
			client.SetCallbackDownload(func(rate speedtest.ByteRate) {
				updateRate(task, "Download", rate, echo.CurrentLatency())
			})
			var err error
			if *multi {
				err = server.MultiDownloadTestContext(ctx, servers)
			} else {
				err = server.DownloadTestContext(ctx)
			}
			if err != nil {
				return err
			}
			echo.Stop()
			task.Printf("Download: %s (Used: %.2fMB)%s", server.DLSpeed, float64(client.GetTotalDownload())/1e6, loadedLatency(echo.Latencies()))
			return nil
		}); err != nil {
			return err
		}
	}
	if !*noUpload {
		if err := runTask(tm, "Upload", func(task *Task) error {
			echo.RunContext(ctx)
			defer echo.Stop()
			client.SetCallbackUpload(func(rate speedtest.ByteRate) {
				updateRate(task, "Upload", rate, echo.CurrentLatency())
			})
			var err error
			if *multi {
				err = server.MultiUploadTestContext(ctx, servers)
			} else {
				err = server.UploadTestContext(ctx)
			}
			if err != nil {
				return err
			}
			echo.Stop()
			task.Printf("Upload: %s (Used: %.2fMB Confirmed: %.1f%%)%s", server.ULSpeed, float64(client.GetTotalUpload())/1e6, client.GetUploadConfirmationRatio()*100, loadedLatency(echo.Latencies()))
			return nil
		}); err != nil {
			return err
		}
	}
	tm.Println("Probe " + server.PacketLoss.String())
	return nil
}

func updateRate(task *Task, name string, rate speedtest.ByteRate, latency int64) {
	if latency <= 0 {
		task.Updatef("%s: %s (Latency: --)", name, rate)
		return
	}
	task.Updatef("%s: %s (Latency: %dms)", name, rate, latency/int64(time.Millisecond))
}

func loadedLatency(samples []int64) string {
	if len(samples) == 0 {
		return " (Latency: --)"
	}
	if len(samples) == 1 {
		return fmt.Sprintf(" (Latency: %dms)", samples[0]/int64(time.Millisecond))
	}
	mean, _, std, minL, maxL := speedtest.StandardDeviation(samples)
	return fmt.Sprintf(" (Latency: %dms Jitter: %dms Min: %dms Max: %dms)", mean/int64(time.Millisecond), std/int64(time.Millisecond), minL/int64(time.Millisecond), maxL/int64(time.Millisecond))
}

func printResults(client *speedtest.Speedtest, servers speedtest.Servers) error {
	if *jsonOutput {
		data, err := client.JSON(servers)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(os.Stdout, string(data))
		return err
	}
	for _, s := range servers {
		data, err := client.JSONL(s)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintln(os.Stdout, string(data)); err != nil {
			return err
		}
	}
	return nil
}

func formatUserInfo(user *speedtest.User, mask bool) string {
	if !mask {
		return user.String()
	}
	if user.Lat != "" && user.Lon != "" {
		return fmt.Sprintf("%s (%s) [%s, %s]", maskSensitiveValue(user.IP), user.Isp, maskSensitiveValue(user.Lat), maskSensitiveValue(user.Lon))
	}
	return fmt.Sprintf("%s (%s) %s", maskSensitiveValue(user.IP), user.Isp, strings.TrimSpace(user.Province+" "+user.City))
}

func maskSensitiveValue(value string) string {
	return strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' {
			return '*'
		}
		return r
	}, value)
}

type AccompanyEcho struct {
	mu             sync.Mutex
	server         *speedtest.Server
	currentLatency atomic.Int64
	interval       time.Duration
	latencies      []int64
	cancel         context.CancelFunc
	done           chan struct{}
}

func newAccompanyEcho(server *speedtest.Server, interval time.Duration) *AccompanyEcho {
	return &AccompanyEcho{server: server, interval: interval}
}

func (ae *AccompanyEcho) Run() { ae.RunContext(context.Background()) }

func (ae *AccompanyEcho) RunContext(parent context.Context) {
	ae.Stop()
	ae.mu.Lock()
	ctx, cancel := context.WithCancel(parent)
	ae.cancel = cancel
	ae.done = make(chan struct{})
	ae.latencies = nil
	ae.currentLatency.Store(0)
	done := ae.done
	ae.mu.Unlock()
	go func() {
		defer close(done)
		ticker := time.NewTicker(ae.interval)
		defer ticker.Stop()
		for {
			samples, err := ae.server.HTTPPing(ctx, 1, 0, nil)
			if err == nil && len(samples) > 0 {
				ae.currentLatency.Store(samples[0])
				ae.mu.Lock()
				ae.latencies = append(ae.latencies, samples[0])
				ae.mu.Unlock()
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (ae *AccompanyEcho) Stop() {
	ae.mu.Lock()
	cancel, done := ae.cancel, ae.done
	ae.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
		ae.mu.Lock()
		if ae.done == done {
			ae.cancel, ae.done = nil, nil
		}
		ae.mu.Unlock()
	}
}

func (ae *AccompanyEcho) CurrentLatency() int64 { return ae.currentLatency.Load() }

func (ae *AccompanyEcho) Latencies() []int64 {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	return append([]int64(nil), ae.latencies...)
}

func showServerList(servers speedtest.Servers) {
	for _, s := range servers {
		latency := "--"
		if s.Latency > 0 {
			latency = fmt.Sprintf("%dms", s.Latency/time.Millisecond)
		}
		fmt.Printf("[%s] %s  %s (%s) by %s\n", s.ID, latency, s.Name, s.Country, s.Sponsor)
	}
}

func parseUnit(value string) speedtest.UnitType {
	switch strings.ToLower(value) {
	case "decimal-bits":
		return speedtest.UnitTypeDecimalBits
	case "decimal-bytes":
		return speedtest.UnitTypeDecimalBytes
	case "binary-bits":
		return speedtest.UnitTypeBinaryBits
	case "binary-bytes":
		return speedtest.UnitTypeBinaryBytes
	default:
		return speedtest.UnitTypeDefaultMbps
	}
}

func parseProto(value string) speedtest.Proto {
	switch strings.ToLower(value) {
	case "icmp":
		return speedtest.ICMP
	case "tcp":
		return speedtest.TCP
	default:
		return speedtest.HTTP
	}
}

func parseTestMode(value string) speedtest.TestMode {
	if strings.EqualFold(value, "tcp") {
		return speedtest.TCPTest
	}
	return speedtest.HTTPTest
}

func containsString(values []string, target string) bool { return slices.Contains(values, target) }

func AppInfo() {
	if !*jsonOutput && !*jsonlOutput {
		fmt.Printf("\n    globalspeedtest-go v%s (git-%s) CAICT\n\n", speedtest.Version(), commit)
	}
}
