// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package cputime is a library that's able to precisely measure on-CPU time for
// goroutines.
package cputime

import (
	"runtime/metrics"
	_ "unsafe" // for go:linkname
)

// TODO(irfansharif): Use go build tags to make this a no-op if not built using
// the modified runtime. Currently we fail with an opaque link error if not
// using the right go SDK.

// Now returns the wall time (in nanoseconds) spent by the current goroutine in
// the running state.
func Now() int64 {
    return grunningnanos()
}

// grunningnanos returns the running time observed by the current goroutine by
// linking to a private symbol in the runtime package.
//
//go:linkname grunningnanos runtime.grunningnanos
func grunningnanos() int64

// metricnanos returns the running time observed by the current goroutine, but
// doing it through an exported metric. It's an alternative to grunningnanos
// that doesn't require the go:linkname directive, though ~5x slower.
func metricnanos() int64 {
	const metric = "/sched/goroutine/running:nanoseconds" // from the modified go runtime

	sample := []metrics.Sample{
		{Name: metric},
	}
	metrics.Read(sample)
	return int64(sample[0].Value.Uint64())
}
