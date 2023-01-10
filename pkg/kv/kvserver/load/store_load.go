// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package load

import "github.com/cockroachdb/cockroach/pkg/util/syncutil"

type StoreLoadStats struct {
	RuntimeCPUNanosPerSecond float64
	ReplicaCPUNanosPerSecond float64
}

type StoreLoadStatsGetter interface {
	Stats() StoreLoadStats
}

// StoreLoad implements the load.RuntimeCPURateListener interface.
type StoreLoad struct {
	mu struct {
		syncutil.Mutex
		last StoreLoadStats
	}
}

// NewStoreLoad...
func NewStoreLoad() *StoreLoad {
	sl := &StoreLoad{}
	sl.mu.last.ReplicaCPUNanosPerSecond = 1
	sl.mu.last.RuntimeCPUNanosPerSecond = 1
	return sl
}

func (sl *StoreLoad) RuntimeCPURate(rate float64) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	sl.mu.last.RuntimeCPUNanosPerSecond = rate
}

// XXX TODO(kvoli): implement this field for multistore
func (sl *StoreLoad) StoresCPURate(rate float64) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	sl.mu.last.ReplicaCPUNanosPerSecond = rate
}

func (sl *StoreLoad) Stats() StoreLoadStats {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	return sl.mu.last
}
