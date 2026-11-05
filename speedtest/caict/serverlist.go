package caict

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
)

//go:embed serverlist_encrypt.json
var embeddedServerList []byte

// Server is one speed test node, matching the decrypted
// serverlist_encrypt.json entries of the app bundles.
type Server struct {
	CityID   string `json:"cityid"`
	GeoID    string `json:"geoid"`
	HostID   string `json:"hostid"`
	HostIP   string `json:"hostip"`
	HostName string `json:"hostname"`
	Location string `json:"location"`
	Oper     string `json:"oper"`
	PName    string `json:"pname"`
	Port     string `json:"port"`
}

// Addr returns "hostip:port" for dialing the node.
func (s Server) Addr() string { return s.HostIP + ":" + s.Port }

func (s Server) String() string {
	return fmt.Sprintf("[%s] %s %s:%s (%s/%s)", s.HostID, s.HostName, s.HostIP, s.Port, s.PName, s.Oper)
}

// ServerList is the decrypted node table.
type ServerList struct {
	Servers []Server
}

// LoadServerList parses decrypted node-list JSON (the plaintext form of
// serverlist_encrypt.json).
func LoadServerList(data []byte) (*ServerList, error) {
	var servers []Server
	if err := json.Unmarshal(data, &servers); err != nil {
		return nil, fmt.Errorf("caict: parse serverlist json: %w", err)
	}
	if len(servers) == 0 {
		return nil, fmt.Errorf("caict: empty serverlist")
	}
	return &ServerList{Servers: servers}, nil
}

// EmbeddedServerList decrypts the bundled serverlist_encrypt.json (722 nodes
// as of v4.4.9), stored in the app's hex DES-ECB format.
func EmbeddedServerList() (*ServerList, error) {
	return LoadEncryptedServerList(embeddedServerList)
}

// LoadEncryptedServerList decrypts a raw serverlist_encrypt.json file
// (hex text payload) and parses it.
func LoadEncryptedServerList(data []byte) (*ServerList, error) {
	plain, err := decryptServerList(string(data))
	if err != nil {
		return nil, err
	}
	return LoadServerList(plain)
}

// Filter returns the nodes matching all non-empty criteria:
// oper (运营商: 移动/电信/联通/教育网/广电网), province (pname),
// and a keyword substring over hostname/location/pname.
func (l *ServerList) Filter(oper, province, keyword string) *ServerList {
	out := &ServerList{}
	keyword = strings.ToLower(keyword)
	for _, s := range l.Servers {
		if oper != "" && s.Oper != oper {
			continue
		}
		if province != "" && !strings.Contains(s.PName, province) {
			continue
		}
		if keyword != "" && !strings.Contains(strings.ToLower(s.HostName+s.Location+s.PName), keyword) {
			continue
		}
		out.Servers = append(out.Servers, s)
	}
	return out
}
