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

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type StoreCPURateListener interface {
	StoreCPURate(rate float64, storeID roachpb.StoreID)
}

type StoreLoadTracker struct {
	mu struct {
		syncutil.Mutex
		storesCPURateListener StoresCPURateListener
		storesCache           map[roachpb.StoreID]float64
		cur                   float64
	}
}

func NewStoreLoadTracker(storesCPURateListener StoresCPURateListener) *StoreLoadTracker {
	slt := &StoreLoadTracker{}
	slt.mu.storesCache = map[roachpb.StoreID]float64{}
	slt.mu.storesCPURateListener = storesCPURateListener
	return slt
}

func (slt *StoreLoadTracker) StoreCPURate(rate float64, storeID roachpb.StoreID) {
	slt.mu.Lock()
	defer slt.mu.Unlock()

	if last, ok := slt.mu.storesCache[storeID]; ok {
		slt.mu.cur -= last
	}

	slt.mu.storesCache[storeID] = rate
	slt.mu.cur += slt.mu.storesCache[storeID]

	slt.mu.storesCPURateListener.StoresCPURate(slt.mu.cur)
}
