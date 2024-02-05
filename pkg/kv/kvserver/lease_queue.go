// Copyright 2024 The Cockroach Authors.
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
	fmt "fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Lease Queue todo
// - [x] basic lease queue rebalancing/preferences operation
// - [x] mass enqueueing into the lease queue on IO overload
// - [x] removing rebalancing lease operations in replicate queue
// - [x] purgatory retries
// - [ ] implement smoothed io metric
// - [ ] gossip on io metric change
// - harmonize replica planner/simulator and lease queue

const (
	// leaseQueueTimerDuration is the duration between (potential) lease transfers of queued
	// replicas. Use a zero duration to process lease transfers greedily.
	leaseQueueTimerDuration = 0
	// leaseQueuePurgatoryCheckInterval is the interval at which replicas in
	// the lease queue purgatory are re-attempted.
	leaseQueuePurgatoryCheckInterval = 10 * time.Second
)

type LeaseQueueMetrics struct{}

type leaseQueue struct {
	allocator allocatorimpl.Allocator
	storePool storepool.AllocatorStorePool
	purgCh    <-chan time.Time
	*baseQueue
}

var _ queueImpl = &leaseQueue{}

func newLeaseQueue(store *Store, allocator allocatorimpl.Allocator) *leaseQueue {
	var storePool storepool.AllocatorStorePool
	if store.cfg.StorePool != nil {
		storePool = store.cfg.StorePool
	}
	lq := &leaseQueue{
		allocator: allocator,
		storePool: storePool,
		purgCh:    time.NewTicker(leaseQueuePurgatoryCheckInterval).C,
	}

	lq.baseQueue = newBaseQueue("lease", lq, store,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           true,
			needsSpanConfigs:     true,
			acceptsUnsplitRanges: false,
			successes:            store.metrics.LeaseQueueSuccesses,
			failures:             store.metrics.LeaseQueueFailures,
			pending:              store.metrics.LeaseQueuePending,
			processingNanos:      store.metrics.LeaseQueueProcessingNanos,
			purgatory:            store.metrics.LeaseQueuePurgatory,
			disabledConfig:       kvserverbase.LeaseQueueEnabled,
		})

	return lq
}

// shouldQueue accepts current time, a replica, and the system config
// and returns whether it should be queued and if so, at what priority.
// The Replica is guaranteed to be initialized.
func (lq *leaseQueue) shouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, repl *Replica, confReader spanconfig.StoreReader,
) (shouldQueue bool, priority float64) {
	leaseStatus := repl.LeaseStatusAt(ctx, now)
	if !leaseStatus.IsValid() {
		// The range has an invalid lease. If this replica is the raft leader then
		// we'd like it to hold a valid lease. We enqueue it regardless of being a
		// leader or follower, where the leader at the time of processing will
		// succeed.
		log.KvDistribution.VEventf(ctx, 2, "invalid lease, enqueuing")
		return true, 0
	}

	if leaseStatus.OwnedBy(repl.StoreID()) && !repl.HasCorrectLeaseType(leaseStatus.Lease) {
		// This replica holds (or held) an incorrect lease type, switch it to the
		// correct type. Typically when changing kv.expiration_leases_only.enabled.
		log.KvDistribution.VEventf(ctx, 2, "incorrect lease type, enqueueing")
		return true, 0
	}

	conf, err := confReader.GetSpanConfigForKey(ctx, repl.startKey)
	if err != nil {
		return false, 0
	}

	// TODO(kvoli): incorporate canTransferLeaseFrom(..) or remove it entirely.
	desc := repl.Desc()
	voterReplicas := desc.Replicas().VoterDescriptors()
	transferLeaseDecision := lq.allocator.ShouldTransferLease(
		ctx,
		lq.storePool,
		desc,
		&conf,
		voterReplicas,
		repl,
		repl.RangeUsageInfo(),
	)

	if transferLeaseDecision.ShouldTransfer() {
		log.KvDistribution.VEventf(ctx, 2, "lease transfer needed, enqueuing")
	}

	return transferLeaseDecision.ShouldTransfer(), transferLeaseDecision.Priority()
}

// process accepts a replica, and the system config and executes
// queue-specific work on it. The Replica is guaranteed to be initialized.
// We return a boolean to indicate if the Replica was processed successfully
// (vs. it being a no-op or an error).
func (lq *leaseQueue) process(
	ctx context.Context, repl *Replica, confReader spanconfig.StoreReader,
) (processed bool, err error) {
	conf, err := confReader.GetSpanConfigForKey(ctx, repl.startKey)
	if err != nil {
		return false, err
	}
	desc := repl.Desc()
	voterReplicas := desc.Replicas().VoterDescriptors()
	rangeUsageInfo := repl.RangeUsageInfo()

	target := lq.allocator.TransferLeaseTarget(
		ctx,
		lq.storePool,
		desc,
		&conf,
		voterReplicas,
		repl,
		repl.RangeUsageInfo(),
		false, /* forceDecisionWithoutStats */
		allocator.TransferLeaseOptions{
			Goal:                   allocator.FollowTheWorkload,
			ExcludeLeaseRepl:       false,
			CheckCandidateFullness: true,
		},
	)

	if target == (roachpb.ReplicaDescriptor{}) {
		// If we don't find a suitable target, but we own a lease violating the
		// lease preferences, and there is a more suitable target, return an error
		// to place the replica in purgatory and retry sooner. This typically
		// happens when we've just acquired a violating lease and we eagerly
		// enqueue the replica before we've received Raft leadership, which
		// prevents us from finding appropriate lease targets since we can't
		// determine if any are behind.
		liveVoters, _ := lq.storePool.LiveAndDeadReplicas(
			voterReplicas, false /* includeSuspectAndDrainingStores */)
		preferred := lq.allocator.PreferredLeaseholders(lq.storePool, &conf, liveVoters)
		if len(preferred) > 0 &&
			repl.LeaseViolatesPreferences(ctx, &conf) {
      log.Infof(ctx, "sending lh to purgatory")
			return false, CantTransferLeaseViolatingPreferencesError{RangeID: desc.RangeID}
		}
		// No suitable targets found and there isn't a more preferred leaseholder.
		return true, nil
	}

	log.Infof(ctx, "transferring lease load=%v to n%ds%d", rangeUsageInfo, target.NodeID, target.StoreID)
	// TODO(kvoli): remove the range mover interface, it is a unnecessary distraction.
	if err := repl.AdminTransferLease(ctx, target.StoreID, false /* bypassSafetyChecks */); err != nil {
		return false, errors.Wrapf(err, "%v: unable to transfer lease to s%d", repl, target)
	}
	return true, nil
}

// processScheduled is called after async task was created to run process.
// This function is called by the process loop synchronously. This method is
// called regardless of process being called or not since replica validity
// checks are done asynchronously.
func (lq *leaseQueue) postProcessScheduled(
	ctx context.Context, replica replicaInQueue, priority float64,
) {
}

// timer returns a duration to wait between processing the next item
// from the queue. The duration of the last processing of a replica
// is supplied as an argument. If no replicas have finished processing
// yet, this can be 0.
func (lq *leaseQueue) timer(time.Duration) time.Duration {
	return leaseQueueTimerDuration
}

// purgatoryChan returns a channel that is signaled with the current
// time when it's time to retry replicas which have been relegated to
// purgatory due to failures. If purgatoryChan returns nil, failing
// replicas are not sent to purgatory.
func (lq *leaseQueue) purgatoryChan() <-chan time.Time {
	// TODO(kvoli): implement purgatory retries for lease preference failures
	// where there is no target due to throttling etc.
	return lq.purgCh
}

// updateChan returns a channel that is signalled whenever there is an update
// to the cluster state that might impact the replicas in the queue's
// purgatory.
func (lq *leaseQueue) updateChan() <-chan time.Time {
	// TODO(kvoli): implement an update callback on gossiped store descriptors.
	return nil
}

// CantTransferLeaseViolatingPreferencesError is an error returned when a lease
// violates the lease preferences, but we couldn't find a valid target to
// transfer the lease to. It indicates that the replica should be sent to
// purgatory, to retry the transfer faster.
type CantTransferLeaseViolatingPreferencesError struct {
	RangeID roachpb.RangeID
}

var _ errors.SafeFormatter = CantTransferLeaseViolatingPreferencesError{}

func (e CantTransferLeaseViolatingPreferencesError) Error() string { return fmt.Sprint(e) }

func (e CantTransferLeaseViolatingPreferencesError) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

func (e CantTransferLeaseViolatingPreferencesError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("can't transfer r%d lease violating preferences, no suitable target", e.RangeID)
	return nil
}

func (CantTransferLeaseViolatingPreferencesError) PurgatoryErrorMarker() {}
