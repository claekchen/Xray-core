package dispatcher

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
)

func TestFlowAccountingRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flow.jsonl")
	t.Setenv(flowLogEnvironment, path)
	flow := newFlowAccounting()

	uplink := buf.New()
	uplink.WriteString("request")
	if err := flow.wrapUplink(buf.Discard).WriteMultiBuffer(buf.MultiBuffer{uplink}); err != nil {
		t.Fatal(err)
	}
	downlink := buf.New()
	downlink.WriteString("response-body")
	if err := flow.wrapDownlink(buf.Discard).WriteMultiBuffer(buf.MultiBuffer{downlink}); err != nil {
		t.Fatal(err)
	}

	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{
		Source: net.TCPDestination(net.ParseAddress("192.0.2.10"), 54321),
		Tag:    "vless-in",
		User:   &protocol.MemoryUser{Email: "primary"},
	})
	ctx = session.ContextWithContent(ctx, &session.Content{Protocol: "tls"})
	outbound := &session.Outbound{
		OriginalTarget: net.TCPDestination(net.ParseAddress("203.0.113.20"), 443),
		Target:         net.TCPDestination(net.ParseAddress("example.com"), 443),
		Tag:            "direct",
	}
	flow.record(flowSnapshotFromContext(ctx, outbound, outbound.Tag))

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var record flowRecord
	if err := json.NewDecoder(bufio.NewReader(file)).Decode(&record); err != nil {
		t.Fatal(err)
	}
	if record.SourceIP != "192.0.2.10" || record.SourcePort != 54321 {
		t.Fatalf("unexpected source: %s:%d", record.SourceIP, record.SourcePort)
	}
	if record.Site != "example.com" || record.TargetPort != 443 {
		t.Fatalf("unexpected target: %s:%d", record.Site, record.TargetPort)
	}
	if record.UplinkBytes != 7 || record.DownlinkBytes != 13 {
		t.Fatalf("unexpected byte counts: up=%d down=%d", record.UplinkBytes, record.DownlinkBytes)
	}
	if record.InboundTag != "vless-in" || record.OutboundTag != "direct" || record.Protocol != "tls" {
		t.Fatalf("unexpected routing metadata: %+v", record)
	}
}

func TestFlowAccountingDisabledWithoutEnvironment(t *testing.T) {
	t.Setenv(flowLogEnvironment, "")
	if flow := newFlowAccounting(); flow != nil {
		t.Fatal("flow accounting must be disabled without an output path")
	}
}
