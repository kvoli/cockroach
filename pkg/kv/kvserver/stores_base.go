// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"strconv"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/monitor"
	"github.com/cockroachdb/errors"
)

// StoresBase is the concrete implementation of kvserverbase.Stores.
type StoresBase struct {
	stores *Stores
}

var _ kvserverbase.StoresIterator = &StoresBase{}

// MakeStoresBase returns a new StoresBase instance.
func MakeStoresBase(stores *Stores) StoresBase {
	return StoresBase{stores}
}

// ForEachStore is part of kvserverbase.StoresIterator.
func (s StoresBase) ForEachStore(f func(kvserverbase.Store) error) error {
	var err error
	s.stores.storeMap.Range(func(k int64, v unsafe.Pointer) bool {
		store := (*Store)(v)

		err = f(MakeStoreBase(store))
		return err == nil
	})
	return err
}

// StoreBase is the concrete implementation of kvserverbase.Store.
type StoreBase struct {
	store *Store
}

var _ kvserverbase.Store = &StoreBase{}

// MakeStoreBase returns a new StoreBase instance.
func MakeStoreBase(store *Store) StoreBase {
	return StoreBase{store}
}

// StoreID is part of kvserverbase.Store.
func (s StoreBase) StoreID() roachpb.StoreID {
	return s.store.StoreID()
}

// Enqueue is part of kvserverbase.Store.
func (s StoreBase) Enqueue(
	ctx context.Context, queue string, rangeID roachpb.RangeID, skipShouldQueue bool,
) error {
	repl, err := s.store.GetReplica(rangeID)
	if err != nil {
		return err
	}

	_, processErr, enqueueErr := s.store.ManuallyEnqueue(ctx, queue, repl, skipShouldQueue)
	if processErr != nil {
		return processErr
	}
	if enqueueErr != nil {
		return enqueueErr
	}
	return nil
}

// SetQueueActive is part of kvserverbase.Store.
func (s StoreBase) SetQueueActive(active bool, queue string) error {
	var kvQueue replicaQueue
	for _, rq := range s.store.scanner.queues {
		if strings.EqualFold(rq.Name(), queue) {
			kvQueue = rq
		}
	}

	if kvQueue == nil {
		return errors.Errorf("unknown queue %q", queue)
	}

	kvQueue.SetDisabled(!active)
	return nil
}

// ReplicaActivity is part of kvserverbase.Store.
func (s StoreBase) ReplicaActivity() []monitor.Activity {
	return s.store.RangeCPUMonitor.Snapshot()
}

// MonitorSlotRange attempts to slot a range into the stores monitor at
// slot.
func (s StoreBase) MonitorSlotRange(rangeID roachpb.RangeID, slot int) bool {
	return s.store.metrics.RangeAccumulatedCPUTime.SetSlot(monitor.Label(rangeID.String()), slot)
}

// MonitorSlotTenant attempts to slot a tenant into the stores monitor at
// slot.
func (s StoreBase) MonitorSlotTenant(tenantID roachpb.TenantID, slot int) bool {
	return s.store.metrics.TenantAccumulatedCPUTime.SetSlot(monitor.Label(tenantID.String()), slot)
}

// RangeSlots returns the currently slotted Ranges
func (s StoreBase) RangeSlots() []roachpb.RangeID {
	slots := s.store.metrics.RangeAccumulatedCPUTime.Slotted()
	ret := make([]roachpb.RangeID, 0)
	for _, v := range slots {
		rangeID, err := strconv.Atoi(string(v))
		if err != nil {
			rangeID = 0
		}
		ret = append(ret, roachpb.RangeID(rangeID))
	}
	return ret
}

// TenantSlots returns the currently slotted Tenants
func (s StoreBase) TenantSlots() []roachpb.TenantID {
	slots := s.store.metrics.TenantAccumulatedCPUTime.Slotted()
	ret := make([]roachpb.TenantID, 0)
	for _, v := range slots {
		tenantID, err := strconv.Atoi(string(v))
		if err != nil || tenantID < 1 {
			// TODO(kvoli): Need to approach this in a better manner.
			tenantID = 666
		}
		ret = append(ret, roachpb.MakeTenantID(uint64(tenantID)))
	}
	return ret

}
