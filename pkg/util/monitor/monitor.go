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

type Label string

// Monitor is a container for labels which may have runs recorded against them.
type Monitor interface {

	// Track registers a label, that may have future Reports called against it.
	Track(l Label)

	// Untrack removes a label.
	Untrack(l Label)

	// Report updates the tracked value associated with a label. In addition,
	// it also updates the aggregate monitor. Currently, this is additive and
	// supports only "Counter" tracking.
	// TODO(kvoli): more general datatypes and aggregation.
	Report(l Label, value int64)

	// Snapshot returns a point in time list of activitiy for all tracked
	// replicas.
	Snapshot() []Activity

	// Get returns the current activity against a single label.
	Get(l Label) Activity

	// Value returns the current aggregation across all tracked labels
	// activity.
	Value() int64
}

// Activity represents a key value pair, between Labels and Values updated
// against them
type Activity struct {
	Label Label
	Value int64
}
