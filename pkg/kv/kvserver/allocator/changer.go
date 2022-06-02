// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocator

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// This interface is an abstraction for the application of changes, that are
// requested by the replicate queue and store rebalancer. The intent here is
// to have a single interface for functions which are impure and execute
// changes, while maintaining logic functions without side effects.
//
// The functions which are called by the store rebalancer and replicate queue
// are listed below.
//
// ReplicateQueue:
// (1) rq.changeReplicas -> replica.ChangeReplicas -> replica.changeReplicasImpl*
//     ChangeReplicas can also call TransferLease, if it is in a joint
//     configuration and leaving, it will call into DB.AdminTransferLease.
//     - rq.addOrReplaceVoters
//     - rq.addOrReplaceNonVoters
//     - rq.removeVoter
//     - rq.removeNonVoter
//     - rq.removeDecommissioning
//     - rq.removeDead
//     - rq.considerRebalance
//
// (2) rq.shedLease -> rq.transferLease
//     - rq.considerRebalance    -> rq.shedLease
//     - rq.maybeTransferLease   -> rq.shedLease
//         - rq.considerRebalance     -> rq.maybeTransferLease
//         - rq.addOrReplaceVoters    -> rq.maybeTransferLease
//         - rq.removeVoter           -> rq.maybeTransferLease
//         - rq.removeDecommissioning -> rq.maybeTransferLease
//
// Store Rebalancer:
// (1) TransferLease 
//     - sr.rebalanceStore -> rq.transferLease -> replica.TransferLease
// (2) replica.RelocateRange
//     This method will call DB.AdminTransferLease and DB.AdminChangeReplicas,
//     this is not ideal as it makes it hard to simulate calls to the DB.
//     - sr.rebalanceStore -> **DB.AdminRelocateRange** -> replica.RelocateRange -> replica.relocateReplicas
//
// implementers currently
// 
// TransferLease
// - replica
// - DB -> replica
// ChangeReplicas
// - DB -> replica
// - replica
// RelocateRange
// - DB -> replica
// - replica
//
// Notes:
// TransferLease and ChangeReplicas are both called directly on the leaseholder
// replica, within the store. These are always mediated by calls to the
// replicateQueue first. We can effectively wrap these calls from the
// ReplicateQueue.
//
// RelocateRange calls both DB.AdminTransferLease() and DB.AdminChangeReplicas
// to actually execute its changes.
//
// So the dependency graph for these functions is:
// (called by)
// 
// 
// r.AdminTransferLease: 
//  - DB.AdminTransferLease
//  - rq.transferLease
//  - rq.adminScatter
// DB.AdminTransferLease
//  - r.relocateReplicas
//  - r.maybeTransferLeaseDuringLeaveJoint
//
// r.AdminChangeReplicas (r.changeReplicasImpl)
//  - rq.changeReplicas
//  - DB.AdminChangeReplicas
// DB.AdminChangeReplicas
//  - r.relocateReplicas
// 
// r.AdminRelocateRange (r.relocateReplicas)
//  - DB.AdminRelocateRange
// DB.AdminRelocateRange
//  - sr.rebalanceStore 
//  - mergequeue.process
//
// r.maybeTransferLeaseDuringLeaveJoint (r.maybeLeaveAtomicChangeReplicas)
//  - r.adminSplitWithDescriptor
//  - r.maybeLeaveAtomicChangeReplicasAndRemoveLearners
//  - r.changeReplicasImpl
// r.maybeLeaveAtomicChangeReplicasAndRemoveLearners
//  - mergequeue.process
//  - r.changeReplicasImpl
//  - r.execReplicationChangesForVoters
//  - r.AdminRelocateRange
//  - rq.processOneChange

type ChangeExecutor interface {
	// TransferLease transfers the lease for the range containing key to the
	// specified target. The target replica for the lease transfer must be one
	// of the existing replicas of the range.
	//
	// key can be either a byte slice or a string.
	//
	// When this method returns, it's guaranteed that the old lease holder has
	// applied the new lease, but that's about it. It's not guaranteed that the
	// new lease holder has applied it (so it might not know immediately that
	// it is the new lease holder).
	TransferLease(ctx context.Context, from roachpb.ReplicaDescriptor, target roachpb.ReplicaDescriptor, rangeQPS float64) error
	//ChangeReplicas adds or removes a set of replicas for a range.
	ChangeReplicas(ctx context.Context, key interface{}, expDesc roachpb.RangeDescriptor, chgs []roachpb.ReplicationChange) error
	// RelocateRange relocates the replicas for a range onto the specified list
	// of stores.
	//
	// Relocate range will call both ChangeReplicas and TransferLease in it's
	// execution, it is a composite of both these functions, transferring the
	// lease and also changing replicas.
	RelocateRange(ctx context.Context, key interface{}, voterTargets, nonVoterTargets []roachpb.ReplicationTarget, transferLeaseToFirstVoter bool) error
}
