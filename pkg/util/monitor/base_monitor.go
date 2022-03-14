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

import "github.com/cockroachdb/cockroach/pkg/util/syncutil"

// BaseMonitor tracks labels and values, assuming aggregation is additive.:
type BaseMonitor struct {
	mu struct {
		syncutil.Mutex
		tracked map[Label]Activity
		sum     int64
	}
}

func NewBaseMonitor() *BaseMonitor {
	monitor := BaseMonitor{}
	monitor.mu.tracked = make(map[Label]Activity, 0)
	monitor.mu.sum = 0
	return &monitor
}

func (b *BaseMonitor) trackLocked(l Label) {
	b.mu.tracked[l] = Activity{Label: l, Value: 0}
}

// Track registers a label, that may have future Reports called against it.
func (b *BaseMonitor) Track(l Label) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.trackLocked(l)
}

// Untrack removes a label and any associated values reported.
func (b *BaseMonitor) Untrack(l Label) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.mu.tracked[l]; !ok {
		return
	}

	b.mu.sum -= b.mu.tracked[l].Value
	delete(b.mu.tracked, l)
}

// Report updates the tracked value associated with a label. In addition,
// it also updates the aggregate monitor. Currently, this is additive and
// supports only "Counter" tracking.
// TODO(kvoli): more general datatypes and aggregation.
func (b *BaseMonitor) Report(l Label, value int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.mu.tracked[l]; !ok {
		b.trackLocked(l)
	}
	activity := b.mu.tracked[l]

	b.mu.sum += value
	activity.Value += value

	b.mu.tracked[l] = activity
}

// Snapshot returns a point in time list of activitiy for all tracked
// replicas.
func (b *BaseMonitor) Snapshot() []Activity {
	b.mu.Lock()
	defer b.mu.Unlock()

	activities := make([]Activity, 0)
	for _, val := range b.mu.tracked {
		activities = append(activities, val)
	}

	return activities
}

// Get returns the current activity against a single label.
func (b *BaseMonitor) Get(l Label) Activity {
	b.mu.Lock()
	defer b.mu.Unlock()

	if activity, ok := b.mu.tracked[l]; ok {
		return activity
	}
	return Activity{}
}

// Value returns the current aggregation across all tracked labels
// activity.
func (b *BaseMonitor) Value() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.mu.sum
}
