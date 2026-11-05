package speedtest

import (
	"context"
	"net"
	"time"

	"github.com/XiaoMengXinX/globalspeedtest-go/speedtest/transport"
)

type PacketLossAnalyzerOptions struct {
	RemoteSamplingInterval time.Duration
	SamplingDuration       time.Duration
	PacketSendingInterval  time.Duration
	PacketSendingTimeout   time.Duration
	SourceInterface        string
	TCPDialer              *net.Dialer
	UDPDialer              *net.Dialer
}

type PacketLossAnalyzer struct {
	options *PacketLossAnalyzerOptions
}

func NewPacketLossAnalyzer(options *PacketLossAnalyzerOptions) *PacketLossAnalyzer {
	if options == nil {
		options = &PacketLossAnalyzerOptions{}
	}
	return &PacketLossAnalyzer{options: options}
}

func (s *Speedtest) NewPacketLossAnalyzer() *PacketLossAnalyzer {
	return NewPacketLossAnalyzer(&PacketLossAnalyzerOptions{
		TCPDialer: s.tcpDialer,
		UDPDialer: s.udpDialer,
	})
}

func (pla *PacketLossAnalyzer) RunMulti(hosts []string) (*transport.PLoss, error) {
	return pla.RunMultiWithContext(context.Background(), hosts)
}

func (pla *PacketLossAnalyzer) RunMultiWithContext(ctx context.Context, hosts []string) (*transport.PLoss, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, transport.ErrUnsupported
}

func (pla *PacketLossAnalyzer) Run(host string, callback func(*transport.PLoss)) error {
	return pla.RunWithContext(context.Background(), host, callback)
}

// RunWithContext reports that CAICT nodes do not expose the packet-loss protocol.
func (pla *PacketLossAnalyzer) RunWithContext(ctx context.Context, host string, callback func(*transport.PLoss)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return transport.ErrUnsupported
}
