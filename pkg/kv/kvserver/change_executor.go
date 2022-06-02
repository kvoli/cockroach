// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type ChangeExecutor struct{
	store *Store
}

// TransferLease() transfers the LeaderLease to another replica. Only the
// current holder of the LeaderLease can do a transfer, because it needs to stop
// serving reads and proposing Raft commands (CPut is a read) while evaluating
// and proposing the TransferLease request. This synchronization with all other
// requests on the leaseholder is enforced through latching. The TransferLease
// request grabs a write latch over all keys in the range.
//
// The method waits for any in-progress lease extension to be done, and it also
// blocks until the transfer is done. If a transfer is already in progress, this
// method joins in waiting for it to complete if it's transferring to the same
// replica. Otherwise, a NotLeaseHolderError is returned.
func (c *ChangeExecutor) TransferLease(
	ctx context.Context,
	from roachpb.ReplicaDescriptor,
	target roachpb.ReplicaDescriptor,
	rangeQPS float64,
) error {
	return nil
}

// Store rebalancer calls relocate range, which calls change replicas
// underneath it. It will select a leaseholder target if pre 22.1 as the first
// argument in the voter targets. Otherwise, the allocator selects targets...
// based
func (c *ChangeExecutor) RelocateRange(
	ctx context.Context,
	key interface{},
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
	transferLeaseToFirstVoter bool,
) error {
	return nil
}

// Change replicas cannot directly change the leaseholder, however
// relocateReplicas which calls ChangeReplicas can.
func (c *ChangeExecutor) ChangeReplicas(
	ctx context.Context,
	key interface{},
	expDesc roachpb.RangeDescriptor,
	chgs []roachpb.ReplicationChange,
) error {
	return nil
}
