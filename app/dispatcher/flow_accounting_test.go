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
	t.Setenv(flowSnapshotIntervalEnvironment, "1h")
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
	flow.start(flowSnapshotFromContext(ctx, outbound, outbound.Tag))
	flow.recordSegment(false)

	moreUplink := buf.New()
	moreUplink.WriteString("more")
	if err := flow.wrapUplink(buf.Discard).WriteMultiBuffer(buf.MultiBuffer{moreUplink}); err != nil {
		t.Fatal(err)
	}
	moreDownlink := buf.New()
	moreDownlink.WriteString("ok")
	if err := flow.wrapDownlink(buf.Discard).WriteMultiBuffer(buf.MultiBuffer{moreDownlink}); err != nil {
		t.Fatal(err)
	}
	flow.finish()

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(bufio.NewReader(file))
	var checkpoint flowRecord
	if err := decoder.Decode(&checkpoint); err != nil {
		t.Fatal(err)
	}
	var final flowRecord
	if err := decoder.Decode(&final); err != nil {
		t.Fatal(err)
	}
	if checkpoint.SourceIP != "192.0.2.10" || checkpoint.SourcePort != 54321 {
		t.Fatalf("unexpected source: %s:%d", checkpoint.SourceIP, checkpoint.SourcePort)
	}
	if checkpoint.Site != "example.com" || checkpoint.TargetPort != 443 {
		t.Fatalf("unexpected target: %s:%d", checkpoint.Site, checkpoint.TargetPort)
	}
	if checkpoint.UplinkBytes != 7 || checkpoint.DownlinkBytes != 13 || checkpoint.Final || checkpoint.Segment != 0 {
		t.Fatalf("unexpected checkpoint: %+v", checkpoint)
	}
	if final.UplinkBytes != 4 || final.DownlinkBytes != 2 || !final.Final || final.Segment != 1 {
		t.Fatalf("unexpected final segment: %+v", final)
	}
	if checkpoint.FlowID == "" || checkpoint.FlowID != final.FlowID {
		t.Fatalf("unexpected flow IDs: %q and %q", checkpoint.FlowID, final.FlowID)
	}
	if checkpoint.InboundTag != "vless-in" || checkpoint.OutboundTag != "direct" || checkpoint.Protocol != "tls" {
		t.Fatalf("unexpected routing metadata: %+v", checkpoint)
	}
}

func TestFlowAccountingDisabledWithoutEnvironment(t *testing.T) {
	t.Setenv(flowLogEnvironment, "")
	if flow := newFlowAccounting(); flow != nil {
		t.Fatal("flow accounting must be disabled without an output path")
	}
}
