// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserverbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/monitor"
	"github.com/cockroachdb/errors"
)

// StoresIterator is able to iterate over all stores on a given node.
type StoresIterator interface {
	ForEachStore(func(Store) error) error
}

// Store is an adapter to the underlying KV store.
type Store interface {
	// StoreID returns the store identifier.
	StoreID() roachpb.StoreID

	// Enqueue the replica with the given range ID into the named queue.
	Enqueue(
		ctx context.Context,
		queue string,
		rangeID roachpb.RangeID,
		skipShouldQueue bool,
	) error

	// SetQueueActive disables/enables the named queue.
	SetQueueActive(active bool, queue string) error

	// ReplicaActivity returns the per-replica activity on this store.
	ReplicaActivity() []monitor.Activity

	// MonitorSlotRange attempts to slot a range into the stores monitor at
	// slot.
	MonitorSlotRange(rangeID roachpb.RangeID, slot int) bool

	// MonitorSlotTenant attempts to slot a tenant into the stores monitor at
	// slot.
	MonitorSlotTenant(tenantID roachpb.TenantID, slot int) bool

	// RangeSlots returns the currently slotted Ranges
	RangeSlots() []roachpb.RangeID

	// TenantSlots returns the currently slotted Tenants
	TenantSlots() []roachpb.TenantID
}

// UnsupportedStoresIterator is a StoresIterator that only returns "unsupported"
// errors.
type UnsupportedStoresIterator struct{}

var _ StoresIterator = UnsupportedStoresIterator{}

// ForEachStore is part of the StoresIterator interface.
func (i UnsupportedStoresIterator) ForEachStore(f func(Store) error) error {
	return errors.New("unsupported in multi-tenancy mode")
}
