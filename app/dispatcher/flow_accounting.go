package dispatcher

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/session"
)

const flowLogEnvironment = "XRAY_FLOW_LOG"

type flowAccounting struct {
	startedAt time.Time
	uplink    atomic.Int64
	downlink  atomic.Int64
}

type flowAccountingWriter struct {
	writer  buf.Writer
	counter *atomic.Int64
}

type flowSnapshot struct {
	SourceIP      string
	SourcePort    uint32
	InboundTag    string
	User          string
	Network       string
	Site          string
	TargetPort    uint32
	OriginalSite  string
	OriginalPort  uint32
	RouteTarget   string
	OutboundTag   string
	Protocol      string
}

type flowRecord struct {
	StartedAt     time.Time `json:"started_at"`
	EndedAt       time.Time `json:"ended_at"`
	DurationMS    int64     `json:"duration_ms"`
	SourceIP      string    `json:"source_ip"`
	SourcePort    uint32    `json:"source_port"`
	InboundTag    string    `json:"inbound_tag"`
	User          string    `json:"user,omitempty"`
	Network       string    `json:"network"`
	Site          string    `json:"site"`
	TargetPort    uint32    `json:"target_port"`
	OriginalSite  string    `json:"original_site,omitempty"`
	OriginalPort  uint32    `json:"original_port,omitempty"`
	RouteTarget   string    `json:"route_target,omitempty"`
	OutboundTag   string    `json:"outbound_tag"`
	Protocol      string    `json:"protocol,omitempty"`
	UplinkBytes   int64     `json:"uplink_bytes"`
	DownlinkBytes int64     `json:"downlink_bytes"`
}

func newFlowAccounting() *flowAccounting {
	if strings.TrimSpace(os.Getenv(flowLogEnvironment)) == "" {
		return nil
	}
	return &flowAccounting{startedAt: time.Now().UTC()}
}

func (f *flowAccounting) wrapUplink(writer buf.Writer) buf.Writer {
	return &flowAccountingWriter{writer: writer, counter: &f.uplink}
}

func (f *flowAccounting) wrapDownlink(writer buf.Writer) buf.Writer {
	return &flowAccountingWriter{writer: writer, counter: &f.downlink}
}

func (w *flowAccountingWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	w.counter.Add(int64(mb.Len()))
	return w.writer.WriteMultiBuffer(mb)
}

func (w *flowAccountingWriter) Close() error {
	return common.Close(w.writer)
}

func (w *flowAccountingWriter) Interrupt() {
	common.Interrupt(w.writer)
}

func flowSnapshotFromContext(ctx context.Context, outbound *session.Outbound, outboundTag string) flowSnapshot {
	snapshot := flowSnapshot{OutboundTag: outboundTag}
	if inbound := session.InboundFromContext(ctx); inbound != nil {
		snapshot.InboundTag = inbound.Tag
		if inbound.Source.IsValid() {
			snapshot.SourceIP = inbound.Source.Address.String()
			snapshot.SourcePort = uint32(inbound.Source.Port)
		}
		if inbound.User != nil {
			snapshot.User = inbound.User.Email
		}
	}
	if outbound != nil {
		if outbound.Target.IsValid() {
			snapshot.Network = outbound.Target.Network.String()
			snapshot.Site = outbound.Target.Address.String()
			snapshot.TargetPort = uint32(outbound.Target.Port)
		}
		if outbound.OriginalTarget.IsValid() {
			snapshot.OriginalSite = outbound.OriginalTarget.Address.String()
			snapshot.OriginalPort = uint32(outbound.OriginalTarget.Port)
		}
		if outbound.RouteTarget.IsValid() {
			snapshot.RouteTarget = outbound.RouteTarget.String()
		}
	}
	if content := session.ContentFromContext(ctx); content != nil {
		snapshot.Protocol = content.Protocol
	}
	return snapshot
}

func (f *flowAccounting) record(snapshot flowSnapshot) {
	endedAt := time.Now().UTC()
	record := flowRecord{
		StartedAt:     f.startedAt,
		EndedAt:       endedAt,
		DurationMS:    endedAt.Sub(f.startedAt).Milliseconds(),
		SourceIP:      snapshot.SourceIP,
		SourcePort:    snapshot.SourcePort,
		InboundTag:    snapshot.InboundTag,
		User:          snapshot.User,
		Network:       snapshot.Network,
		Site:          snapshot.Site,
		TargetPort:    snapshot.TargetPort,
		OriginalSite:  snapshot.OriginalSite,
		OriginalPort:  snapshot.OriginalPort,
		RouteTarget:   snapshot.RouteTarget,
		OutboundTag:   snapshot.OutboundTag,
		Protocol:      snapshot.Protocol,
		UplinkBytes:   f.uplink.Load(),
		DownlinkBytes: f.downlink.Load(),
	}
	data, err := json.Marshal(record)
	if err != nil {
		errors.LogWarning(context.Background(), "failed to encode flow accounting record: ", err)
		return
	}
	path := strings.TrimSpace(os.Getenv(flowLogEnvironment))
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		errors.LogWarning(context.Background(), "failed to open flow accounting log: ", err)
		return
	}
	data = append(data, '\n')
	if _, err := file.Write(data); err != nil {
		errors.LogWarning(context.Background(), "failed to write flow accounting record: ", err)
	}
	if err := file.Close(); err != nil {
		errors.LogWarning(context.Background(), "failed to close flow accounting log: ", err)
	}
}
