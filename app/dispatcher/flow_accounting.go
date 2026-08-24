package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/session"
)

const (
	flowLogEnvironment              = "XRAY_FLOW_LOG"
	flowSnapshotIntervalEnvironment = "XRAY_FLOW_SNAPSHOT_INTERVAL"
	defaultFlowSnapshotInterval     = 5 * time.Minute
)

var (
	flowProcessID = time.Now().UTC().UnixNano()
	flowSequence  atomic.Uint64
)

type flowAccounting struct {
	startedAt       time.Time
	intervalStarted time.Time
	flowID          string
	uplink          atomic.Int64
	downlink        atomic.Int64
	lastUplink      int64
	lastDownlink    int64
	segment         uint64
	snapshot        flowSnapshot
	done            chan struct{}
	startOnce       sync.Once
	finishOnce      sync.Once
	mu              sync.Mutex
	finished        bool
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
	FlowID         string    `json:"flow_id"`
	Segment        uint64    `json:"segment"`
	Final          bool      `json:"final"`
	StartedAt     time.Time `json:"started_at"`
	IntervalStartedAt time.Time `json:"interval_started_at"`
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
	startedAt := time.Now().UTC()
	sequence := flowSequence.Add(1)
	return &flowAccounting{
		startedAt:       startedAt,
		intervalStarted: startedAt,
		flowID:          fmt.Sprintf("%d-%d", flowProcessID, sequence),
		done:            make(chan struct{}),
	}
}

func flowSnapshotInterval() time.Duration {
	value := strings.TrimSpace(os.Getenv(flowSnapshotIntervalEnvironment))
	if value == "" {
		return defaultFlowSnapshotInterval
	}
	interval, err := time.ParseDuration(value)
	if err != nil || interval <= 0 {
		errors.LogWarning(context.Background(), "invalid flow snapshot interval, using default: ", value)
		return defaultFlowSnapshotInterval
	}
	return interval
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

func (f *flowAccounting) start(snapshot flowSnapshot) {
	f.startOnce.Do(func() {
		f.mu.Lock()
		f.snapshot = snapshot
		f.mu.Unlock()
		go f.runSnapshots(flowSnapshotInterval())
	})
}

func (f *flowAccounting) runSnapshots(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			f.recordSegment(false)
		case <-f.done:
			return
		}
	}
}

func (f *flowAccounting) finish() {
	f.finishOnce.Do(func() {
		close(f.done)
		f.recordSegment(true)
	})
}

func (f *flowAccounting) recordSegment(final bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.finished {
		return
	}
	endedAt := time.Now().UTC()
	uplink := f.uplink.Load()
	downlink := f.downlink.Load()
	uplinkDelta := uplink - f.lastUplink
	downlinkDelta := downlink - f.lastDownlink
	if !final && uplinkDelta == 0 && downlinkDelta == 0 {
		return
	}
	record := flowRecord{
		FlowID:         f.flowID,
		Segment:        f.segment,
		Final:          final,
		StartedAt:     f.startedAt,
		IntervalStartedAt: f.intervalStarted,
		EndedAt:       endedAt,
		DurationMS:    endedAt.Sub(f.intervalStarted).Milliseconds(),
		SourceIP:      f.snapshot.SourceIP,
		SourcePort:    f.snapshot.SourcePort,
		InboundTag:    f.snapshot.InboundTag,
		User:          f.snapshot.User,
		Network:       f.snapshot.Network,
		Site:          f.snapshot.Site,
		TargetPort:    f.snapshot.TargetPort,
		OriginalSite:  f.snapshot.OriginalSite,
		OriginalPort:  f.snapshot.OriginalPort,
		RouteTarget:   f.snapshot.RouteTarget,
		OutboundTag:   f.snapshot.OutboundTag,
		Protocol:      f.snapshot.Protocol,
		UplinkBytes:   uplinkDelta,
		DownlinkBytes: downlinkDelta,
	}
	if writeFlowRecord(record) {
		f.lastUplink = uplink
		f.lastDownlink = downlink
		f.intervalStarted = endedAt
		f.segment++
	}
	if final {
		f.finished = true
	}
}

func writeFlowRecord(record flowRecord) bool {
	data, err := json.Marshal(record)
	if err != nil {
		errors.LogWarning(context.Background(), "failed to encode flow accounting record: ", err)
		return false
	}
	path := strings.TrimSpace(os.Getenv(flowLogEnvironment))
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		errors.LogWarning(context.Background(), "failed to open flow accounting log: ", err)
		return false
	}
	data = append(data, '\n')
	success := true
	if _, err := file.Write(data); err != nil {
		errors.LogWarning(context.Background(), "failed to write flow accounting record: ", err)
		success = false
	}
	if err := file.Close(); err != nil {
		errors.LogWarning(context.Background(), "failed to close flow accounting log: ", err)
		success = false
	}
	return success
}
