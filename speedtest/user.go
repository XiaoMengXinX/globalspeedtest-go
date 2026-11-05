package speedtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const controlUserAgent = "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko"

// User represents the caller's public address and region.
type User struct {
	IP       string `xml:"ip,attr"`
	Lat      string `xml:"lat,attr"`
	Lon      string `xml:"lon,attr"`
	Isp      string `xml:"isp,attr"`
	Country  string `xml:"country,attr"`
	Province string
	City     string
	Operator string
}

// Users preserves the client information container.
type Users struct {
	Users []User `xml:"client"`
}

type userLookup struct {
	done chan struct{}
	user *User
	err  error
}

func (s *Speedtest) controlGet(ctx context.Context, path string, query url.Values) ([]byte, error) {
	u, err := url.Parse(s.config.ControlURL)
	if err != nil {
		return nil, err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return nil, errors.New("invalid control URL")
	}
	u.Path, u.RawPath, u.RawQuery, u.Fragment = path, "", query.Encode(), ""
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", controlUserAgent)
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := s.doer.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("control request: HTTP %d", resp.StatusCode)
	}
	const limit = 4 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if len(body) > limit {
		return nil, errors.New("control response too large")
	}
	return body, ctx.Err()
}

func countryCode(country string) string {
	switch strings.ToLower(strings.TrimSpace(country)) {
	case "中国", "中国大陆", "中华人民共和国", "china", "cn":
		return "CN"
	case "香港", "中国香港", "hong kong", "hk":
		return "HK"
	case "澳门", "中国澳门", "macao", "macau", "mo":
		return "MO"
	case "台湾", "中国台湾", "taiwan", "tw":
		return "TW"
	default:
		return strings.ToUpper(strings.TrimSpace(country))
	}
}

func (s *Speedtest) fetchUser(ctx context.Context) (*User, error) {
	body, err := s.controlGet(ctx, "/dataServer/getIpLocSP.php", url.Values{
		"tag": {"ios"}, "mac": {s.config.DeviceID},
	})
	if err != nil {
		return nil, err
	}
	parts := strings.Split(strings.TrimSpace(string(body)), "|")
	if len(parts) < 4 || net.ParseIP(strings.TrimSpace(parts[0])) == nil {
		return nil, errors.New("invalid public IP location response")
	}
	var region []string
	if err := json.Unmarshal([]byte(parts[1]), &region); err != nil || len(region) < 3 {
		return nil, errors.New("invalid public IP region")
	}
	u := &User{
		IP: strings.TrimSpace(parts[0]), Country: countryCode(region[0]),
		Province: region[1], City: region[2], Operator: strings.TrimSpace(parts[3]),
	}
	if u.Operator == "" && len(region) > 4 {
		u.Operator = region[4]
	}
	u.Isp = u.Operator
	if loc := s.config.Location; loc != nil {
		u.Lat = strconv.FormatFloat(loc.Lat, 'f', -1, 64)
		u.Lon = strconv.FormatFloat(loc.Lon, 'f', -1, 64)
		if loc.CC != "" {
			u.Country = countryCode(loc.CC)
		}
	}
	return u, nil
}

// FetchUserInfo returns the caller's public address and region.
func (s *Speedtest) FetchUserInfo() (*User, error) {
	return s.FetchUserInfoContext(context.Background())
}

func FetchUserInfo() (*User, error) {
	return defaultClient.FetchUserInfo()
}

// FetchUserInfoContext shares one location lookup across concurrent callers.
func (s *Speedtest) FetchUserInfoContext(ctx context.Context) (*User, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.userMu.Lock()
	lookup := s.userLookup
	if lookup != nil {
		s.userMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-lookup.done:
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return lookup.user, lookup.err
		}
	}
	lookup = &userLookup{done: make(chan struct{})}
	s.userLookup = lookup
	s.userMu.Unlock()

	u, err := s.fetchUser(ctx)
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	s.userMu.Lock()
	lookup.user, lookup.err = u, err
	if s.userLookup == lookup {
		if err != nil {
			s.userLookup = nil
		} else {
			s.User = u
		}
	}
	close(lookup.done)
	s.userMu.Unlock()
	return u, err
}

func FetchUserInfoContext(ctx context.Context) (*User, error) {
	return defaultClient.FetchUserInfoContext(ctx)
}

func (u *User) String() string {
	return fmt.Sprintf("%s (%s) %s", u.IP, u.Isp, strings.TrimSpace(u.Country+" "+u.Province+" "+u.City))
}
