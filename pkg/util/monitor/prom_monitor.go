// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package monitor

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/gogo/protobuf/proto"
	io_prometheus_client "github.com/prometheus/client_model/go"
	prometheusgo "github.com/prometheus/client_model/go"
)

// PromMonitor tracks a monitor and exposes a fixed number of tracked labels.
type PromMonitor struct {
	mu    struct {
		syncutil.Mutex
		slots    []Label
		k        int
		interval time.Duration
		nextT    time.Time
	}
	metric.Metadata
	Monitor
}

var _ metric.Iterable = (*PromMonitor)(nil)
var _ metric.PrometheusIterable = (*PromMonitor)(nil)
var _ metric.PrometheusExportable = (*PromMonitor)(nil)

func NewPromMonitor(m Monitor, metadata metric.Metadata) *PromMonitor {
	p := PromMonitor{
		Metadata: metadata,
		Monitor:  m,
	}
	p.mu.k = 10
	p.mu.slots = make([]Label, p.mu.k)
	p.mu.interval = time.Second * 10
	p.mu.nextT = time.Now()
	return &p
}

// GetName is part of the metric.Iterable interface.
func (p *PromMonitor) GetName() string {
	return p.Metadata.GetName()

}

// GetHelp is part of the metric.Iterable interface.
func (p *PromMonitor) GetHelp() string {
	return p.Metadata.GetHelp()
}

// GetMeasurement is part of the metric.Iterable interface.
func (p *PromMonitor) GetMeasurement() string {
	return p.Metadata.GetMeasurement()
}

// GetUnit is part of the metric.Iterable interface.
func (p *PromMonitor) GetUnit() metric.Unit {
	return p.Metadata.GetUnit()
}

// GetMetadata is part of the metric.Iterable interface.
func (p *PromMonitor) GetMetadata() metric.Metadata {
	baseMetadata := p.Metadata
	baseMetadata.MetricType = prometheusgo.MetricType_COUNTER
	return baseMetadata
}

// Inspect is part of the metric.Iterable interface.
func (p *PromMonitor) Inspect(f func(interface{})) {
	f(p)
}

// GetType is part of the metric.PrometheusExportable interface.
func (p *PromMonitor) GetType() *io_prometheus_client.MetricType {
	return prometheusgo.MetricType_COUNTER.Enum()
}

// GetLabels is part of the metric.PrometheusExportable interface.
func (p *PromMonitor) GetLabels() []*io_prometheus_client.LabelPair {
	return p.Metadata.GetLabels()
}

// ToPrometheusMetric constructs a prometheus metric for this Counter.
func (p *PromMonitor) ToPrometheusMetric() *io_prometheus_client.Metric {
	return &io_prometheus_client.Metric{
		Counter: &io_prometheus_client.Counter{
			Value: proto.Float64(float64(p.Value())),
		},
	}
}

// SetSlot() allocates a label to a slot. Labels in slots are exported to
// prometheus. Returns true if slotting was successful, false otherwise.
// Setting a slot to an empty label will disable exporting that slot and remove
// any previously exported labels.
func (p *PromMonitor) SetSlot(l Label, i int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if i < p.mu.k {
		p.mu.slots[i] = l
		return true
	}
	return false
}

func (p *PromMonitor) Each(
	labels []*io_prometheus_client.LabelPair, f func(metric *io_prometheus_client.Metric),
) {
	p.mu.Lock()
	defer p.mu.Unlock()

	processSlot := func(slot int, l Label) {
		if pm := p.labelPrometheusMetric(l); pm != nil {
			slotLabel := fmt.Sprintf("slot")
			slotValue := fmt.Sprintf("%d", slot)

			childLabels := make([]*io_prometheus_client.LabelPair, 0, len(labels))
			childLabels = append(childLabels, labels...)
			childLabels = append(childLabels, &io_prometheus_client.LabelPair{Name: &slotLabel, Value: &slotValue})
			pm.Label = childLabels
			f(pm)
		}
	}

	// Explicitly Exported
	for i, label := range p.mu.slots {
		if label != "" {
			processSlot(i, label)
		}
	}
}

// ToPrometheusMetric takes a Label (string) and a value, returning a prometheus metric.
func (p *PromMonitor) labelPrometheusMetric(l Label) *io_prometheus_client.Metric {
	if activity := p.Monitor.Get(l); activity.Label != "" {
		return &io_prometheus_client.Metric{
			Counter: &io_prometheus_client.Counter{
				Value: proto.Float64(float64(activity.Value)),
			},
		}
	}
	return nil
}

func (p *PromMonitor) Slotted() []Label {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.mu.slots
}
