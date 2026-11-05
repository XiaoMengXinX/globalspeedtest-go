package speedtest

import (
	"context"
	"errors"
	"testing"

	"github.com/XiaoMengXinX/globalspeedtest-go/speedtest/transport"
)

func TestPacketLossAnalyzerReportsUnsupported(t *testing.T) {
	analyzer := New().NewPacketLossAnalyzer()
	called := false
	callback := func(*transport.PLoss) { called = true }
	if err := analyzer.Run("127.0.0.1:1", callback); !errors.Is(err, transport.ErrUnsupported) {
		t.Fatalf("Run error=%v", err)
	}
	if result, err := analyzer.RunMulti([]string{"127.0.0.1:1"}); result != nil || !errors.Is(err, transport.ErrUnsupported) {
		t.Fatalf("RunMulti result=%v error=%v", result, err)
	}
	if called {
		t.Fatal("unsupported analyzer emitted a loss sample")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := analyzer.RunWithContext(ctx, "127.0.0.1:1", callback); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Run error=%v", err)
	}
	if _, err := analyzer.RunMultiWithContext(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled RunMulti error=%v", err)
	}
}
