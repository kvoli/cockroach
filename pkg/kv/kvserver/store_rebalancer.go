// Copyright 2018 The Cockroach Authors.
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
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"go.etcd.io/etcd/raft/v3"
)

var (
	metaStoreRebalancerLeaseTransferCount = metric.Metadata{
		Name:        "rebalancing.lease.transfers",
		Help:        "Number of lease transfers motivated by store-level load imbalances",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}
	metaStoreRebalancerRangeRebalanceCount = metric.Metadata{
		Name:        "rebalancing.range.rebalances",
		Help:        "Number of range rebalance operations motivated by store-level load imbalances",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}
)

// StoreRebalancerMetrics is the set of metrics for the store-level rebalancer.
type StoreRebalancerMetrics struct {
	LeaseTransferCount  *metric.Counter
	RangeRebalanceCount *metric.Counter
}

func makeStoreRebalancerMetrics() StoreRebalancerMetrics {
	return StoreRebalancerMetrics{
		LeaseTransferCount:  metric.NewCounter(metaStoreRebalancerLeaseTransferCount),
		RangeRebalanceCount: metric.NewCounter(metaStoreRebalancerRangeRebalanceCount),
	}
}

// LoadBasedRebalancingMode controls whether range rebalancing takes
// additional variables such as write load and disk usage into account.
// If disabled, rebalancing is done purely based on replica count.
var LoadBasedRebalancingMode = settings.RegisterEnumSetting(
	settings.SystemOnly,
	"kv.allocator.load_based_rebalancing",
	"whether to rebalance based on the distribution of QPS across stores",
	"leases and replicas",
	map[int64]string{
		int64(LBRebalancingOff):               "off",
		int64(LBRebalancingLeasesOnly):        "leases",
		int64(LBRebalancingLeasesAndReplicas): "leases and replicas",
	},
).WithPublic()

// LBRebalancingMode controls if and when we do store-level rebalancing
// based on load.
type LBRebalancingMode int64

const (
	// LBRebalancingOff means that we do not do store-level rebalancing
	// based on load statistics.
	LBRebalancingOff LBRebalancingMode = iota
	// LBRebalancingLeasesOnly means that we rebalance leases based on
	// store-level QPS imbalances.
	LBRebalancingLeasesOnly
	// LBRebalancingLeasesAndReplicas means that we rebalance both leases and
	// replicas based on store-level QPS imbalances.
	LBRebalancingLeasesAndReplicas
)

// StoreRebalancer is responsible for examining how the associated store's load
// compares to the load on other stores in the cluster and transferring leases
// or replicas away if the local store is overloaded.
//
// This isn't implemented as a Queue because the Queues all operate on one
// replica at a time, making a local decision about each replica. Queues don't
// really know how the replica they're looking at compares to other replicas on
// the store. Our goal is balancing stores, though, so it's preferable to make
// decisions about each store and then carefully pick replicas to move that
// will best accomplish the store-level goals.
type StoreRebalancer struct {
	log.AmbientContext
	metrics         StoreRebalancerMetrics
	st              *cluster.Settings
	rq              *replicateQueue
	replRankings    *replicaRankings
	getRaftStatusFn func(replica *Replica) *raft.Status
}

// NewStoreRebalancer creates a StoreRebalancer to work in tandem with the
// provided replicateQueue.
func NewStoreRebalancer(
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	rq *replicateQueue,
	replRankings *replicaRankings,
) *StoreRebalancer {
	sr := &StoreRebalancer{
		AmbientContext: ambientCtx,
		metrics:        makeStoreRebalancerMetrics(),
		st:             st,
		rq:             rq,
		replRankings:   replRankings,
		getRaftStatusFn: func(replica *Replica) *raft.Status {
			return replica.RaftStatus()
		},
	}
	sr.AddLogTag("store-rebalancer", nil)
	sr.rq.store.metrics.registry.AddMetricStruct(&sr.metrics)
	return sr
}

// Start runs an infinite loop in a goroutine which regularly checks whether
// the store is overloaded along any important dimension (e.g. range count,
// QPS, disk usage), and if so attempts to correct that by moving leases or
// replicas elsewhere.
//
// This worker acts on store-level imbalances, whereas the replicate queue
// makes decisions based on the zone config constraints and diversity of
// individual ranges. This means that there are two different workers that
// could potentially be making decisions about a given range, so they have to
// be careful to avoid stepping on each others' toes.
//
// TODO(a-robinson): Expose metrics to make this understandable without having
// to dive into logspy.
func (sr *StoreRebalancer) Start(ctx context.Context, stopper *stop.Stopper) {
	ctx = sr.AnnotateCtx(ctx)

	// Start a goroutine that watches and proactively renews certain
	// expiration-based leases.
	_ = stopper.RunAsyncTask(ctx, "store-rebalancer", func(ctx context.Context) {
		timer := timeutil.NewTimer()
		defer timer.Stop()
		timer.Reset(jitteredInterval(allocator.LoadBasedRebalanceInterval.Get(&sr.st.SV)))
		for {
			// Wait out the first tick before doing anything since the store is still
			// starting up and we might as well wait for some qps/wps stats to
			// accumulate.
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				timer.Read = true
				timer.Reset(jitteredInterval(allocator.LoadBasedRebalanceInterval.Get(&sr.st.SV)))
			}

			mode := LBRebalancingMode(LoadBasedRebalancingMode.Get(&sr.st.SV))
			if mode == LBRebalancingOff {
				continue
			}

			storeList, _, _ := sr.rq.store.cfg.StorePool.GetStoreList(storepool.StoreFilterSuspect)
			sr.rebalanceStore(ctx, mode, storeList)
		}
	})
}

// NB: The StoreRebalancer only cares about the convergence of QPS across
// stores, not the convergence of range count. So, we don't use the allocator's
// `scorerOptions` here, which sets the range count rebalance threshold.
// Instead, we use our own implementation of `scorerOptions` that promotes QPS
// balance.
func (sr *StoreRebalancer) scorerOptions(ctx context.Context) *allocatorimpl.QPSScorerOptions {
	return &allocatorimpl.QPSScorerOptions{
		StoreHealthOptions:    sr.rq.allocator.StoreHealthOptions(ctx),
		Deterministic:         sr.rq.store.cfg.StorePool.Deterministic,
		QPSRebalanceThreshold: allocator.QPSRebalanceThreshold.Get(&sr.st.SV),
		MinRequiredQPSDiff:    allocator.MinQPSDifferenceForTransfers.Get(&sr.st.SV),
	}
}

// rebalanceStore iterates through the top K hottest ranges on this store and
// for each such range, performs a lease transfer if it determines that that
// will improve QPS balance across the stores in the cluster. After it runs out
// of leases to transfer away (i.e. because it couldn't find better
// replacements), it considers these ranges for replica rebalancing.
//
// TODO(aayush): We don't try to move replicas or leases away from the local
// store unless it is fielding more than the overfull threshold of QPS based off
// of all the stores in the cluster. Is this desirable? Should we be more
// aggressive?
func (sr *StoreRebalancer) rebalanceStore(
	ctx context.Context, mode LBRebalancingMode, allStoresList storepool.StoreList,
) {
	options := sr.scorerOptions(ctx)
	var localDesc *roachpb.StoreDescriptor
	for i := range allStoresList.Stores {
		if allStoresList.Stores[i].StoreID == sr.rq.store.StoreID() {
			localDesc = &allStoresList.Stores[i]
			break
		}
	}
	if localDesc == nil {
		log.Warningf(ctx, "StorePool missing descriptor for local store")
		return
	}

	// We only bother rebalancing stores that are fielding more than the
	// cluster-level overfull threshold of QPS.
	qpsMaxThreshold := allocatorimpl.OverfullQPSThreshold(options, allStoresList.CandidateQueriesPerSecond.Mean)
	if !(localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold) {
		log.Infof(ctx, "local QPS %.2f is below max threshold %.2f (mean=%.2f); no rebalancing needed",
			localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold, allStoresList.CandidateQueriesPerSecond.Mean)
		return
	}

	for {
		var replicasToMaybeRebalance []replicaWithStats
		storeMap := allStoresList.ToMap()

		// First check if we should transfer leases away to better balance QPS.
		log.Infof(ctx,
			"considering load-based lease transfers for s%d with %.2f qps (mean=%.2f, upperThreshold=%.2f)",
			localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, allStoresList.CandidateQueriesPerSecond.Mean, qpsMaxThreshold)
		hottestRanges := sr.replRankings.topQPS()
		for localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold {
			replWithStats, target, considerForRebalance := sr.chooseLeaseToTransfer(
				ctx,
				&hottestRanges,
				localDesc,
				allStoresList,
				storeMap,
				sr.scorerOptions(ctx),
			)
			replicasToMaybeRebalance = append(replicasToMaybeRebalance, considerForRebalance...)
			if replWithStats.repl == nil {
				break
			}

			timeout := sr.rq.processTimeoutFunc(sr.st, replWithStats.repl)
			if err := contextutil.RunWithTimeout(ctx, "transfer lease", timeout, func(ctx context.Context) error {
				return sr.rq.transferLease(ctx, replWithStats.repl, target, replWithStats.qps)
			}); err != nil {
				log.Errorf(ctx, "unable to transfer lease to s%d: %+v", target.StoreID, err)
				continue
			}
			sr.metrics.LeaseTransferCount.Inc(1)

			// Finally, update our local copies of the descriptors so that if
			// additional transfers are needed we'll be making the decisions with more
			// up-to-date info. The StorePool copies are updated by transferLease.
			localDesc.Capacity.LeaseCount--
			localDesc.Capacity.QueriesPerSecond -= replWithStats.qps
			if otherDesc := storeMap[target.StoreID]; otherDesc != nil {
				otherDesc.Capacity.LeaseCount++
				otherDesc.Capacity.QueriesPerSecond += replWithStats.qps
			}
		}

		if !(localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold) {
			log.Infof(ctx,
				"load-based lease transfers successfully brought s%d down to %.2f qps (mean=%.2f, upperThreshold=%.2f)",
				localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, allStoresList.CandidateQueriesPerSecond.Mean, qpsMaxThreshold)
			return
		}

		if mode != LBRebalancingLeasesAndReplicas {
			log.Infof(ctx,
				"ran out of leases worth transferring and qps (%.2f) is still above desired threshold (%.2f)",
				localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold)
			return
		}

		log.Infof(ctx,
			"ran out of leases worth transferring and qps (%.2f) is still above desired threshold (%.2f); considering load-based replica rebalances",
			localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold)

		// Re-combine replicasToMaybeRebalance with what remains of hottestRanges so
		// that we'll reconsider them for replica rebalancing.
		replicasToMaybeRebalance = append(replicasToMaybeRebalance, hottestRanges...)

		replWithStats, voterTargets, nonVoterTargets, chgs := sr.chooseRangeToRebalance(
			ctx,
			&replicasToMaybeRebalance,
			localDesc,
			allStoresList,
			sr.scorerOptions(ctx),
		)

		if replWithStats.repl == nil {
			log.Infof(ctx,
				"ran out of replicas worth transferring and qps (%.2f) is still above desired threshold (%.2f); will check again soon",
				localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold)
			break
		}

		descBeforeRebalance := replWithStats.repl.Desc()
		log.VEventf(
			ctx,
			1,
			"rebalancing r%d (%.2f qps) to better balance load: voters from %v to %v; non-voters from %v to %v, using changes %v",
			replWithStats.repl.RangeID,
			replWithStats.qps,
			descBeforeRebalance.Replicas().Voters(),
			voterTargets,
			descBeforeRebalance.Replicas().NonVoters(),
			nonVoterTargets,
			chgs,
		)

		timeout := sr.rq.processTimeoutFunc(sr.st, replWithStats.repl)
		if err := contextutil.RunWithTimeout(ctx, "change replicas", timeout, func(ctx context.Context) error {
			return sr.rq.changeReplicas(
				ctx,
				replWithStats.repl,
				chgs,
				descBeforeRebalance,
				kvserverpb.SnapshotRequest_REBALANCE,
				kvserverpb.ReasonRebalance,
				"",
				false,
			)
		}); err != nil {
			log.Errorf(ctx, "unable to relocate range to %v: %v", chgs, err)
			continue
		}
		sr.metrics.RangeRebalanceCount.Inc(1)
	}

	log.Infof(ctx,
		"load-based replica transfers successfully brought s%d down to %.2f qps (mean=%.2f, upperThreshold=%.2f)",
		localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, allStoresList.CandidateQueriesPerSecond.Mean, qpsMaxThreshold)
}

// store rebalancer
// phase(1)
// while qps > max threshold and a target hot range exists
//   transfer lease to replica with lowest qps for hot range
//
// (phase 2)
//
// while qps > max threshold
//   get new replica set, using the allocator add target and remove target N times
//   call relocate range and send lease to store with lowest qps
//      change replicas one at a time
//      transfer the lease to the first target
//   subtract the qps from ourselves (this implicitly assumes that we
//   are always transferring the lease to a different store). We also
//   rely on this assumption to exit the loop.
//
//  when change replicas in the second half, we rely on two
//  features of relocate range. (i) The ability to remove a
//  leaseholder but continue changes replicas one by one. (ii) The
//  allocator check that relocateRange performs to ensure
//  intermediate steps don't violate configs.
//
//  (ii) could just as easily be done in the store rebalancer loop.
//  Create the target final replica set, then allocate out of it in
//  some order.
//  (i) cannot really be done by using the replicate queue, change
//  replicas alone as we require the lease to process the change.
//  However, this is not a bad thing, lease transfers should be
//  strictly limited to the first phase.
//
// issues
// (1) lease transfer doesn't respect lease preferences (note that we
//     could select a target using the allocator, then set it to be the
//     first element, to transfer the lease to. This would maybe
//     resolve the issue).
// (2) relocate range changes replicas and leases, containing a wrapper
// 	   around a lot of logic that is also "part of the algorithm". For
//     example, the order in which we decide to add/remove replicas, which
//     lease transfers occur in the intermediate stages.
// (3) simulating relocate range is difficult due to the additional
//     logic it contains.
// (4) the order of replica changes inside relocate range, is
//     dictated not by qps but rather range count.
//
// options
// a. explicitly perform a lease transfer and do the change replica calls
//    ourselves, in the store rebalancer loop.
//    - We need to maintain the lease here explicitly, so after a
//      transfer occurs, we are not able to locally issue replica
//      changes.
//    - Duplicates the logic of relocate range, whilst still
//      maintaining it elsewhere.
// b. disable all lease transfers in the second half, instead only
//    allow replica changes - loop back to lease transfers after each
//    replica change immediately to enable lease transfers.
//    - this works however it could be inefficient.
// c. simulate relocate range and leave the production code as is -
//    using relocateOne, refactoring relocateOne.
//    - lowest risk to the existing algorithm, however the most work
//      involved here.
//
//
// (b) will change the effects of the store rebalancer, however we
// could show that this is not strictly a bad thing. A more detailed solution follows:
//
//    Algorithm RebalanceStore:
//
//      for inf
//
//        # transfer leases until either
//        # (1) localQPS is <= threshold OR
//        # (2) no suitable lease transfers
//        for localQPS > threshold:
//          target, repl <- transferLeaseTarget
//
//          if not repl then break
//
//          transferLease(target, repl)
//        done
//
//        # make a replica change, if successful then
//  	  # try transferring again
//        add, remove, range <- chooseRangeToRebalance
//
//        # no replica change found, end algorithm
//        if not range then break
//
//        potentialNewLeaseTransfers <- true
//        changeReplicas(add, remove...)
//
//      done
//
//
//
// This should enable lease transfers to occur, however only within the first phase.
// Normally rebalanceStore is called

// RebalanceStore repeatedly calls NextRebalanceAction until it returns nil.
//
// Arguments:
//   storelist, mode
// Returns:
//    nil
// Type:
//     agent (side-effects, calls algorithm to decide what to do)
//
// func (sr *StoreRebalancer) RebalanceStore() {}

// NextRebalanceAction returns the next rebalance action to take
// Arguments:
//    storelist, mode
// Returns:
//    action {transfer | repl change | noop}
// Type:
//     algorithm (no side-effects)
// func (sr *StoreRebalancer) NextAction() {}
//
// TODO(kvoli): create a dashboard to run when doing "balance tests".

func (sr *StoreRebalancer) chooseLeaseToTransfer(
	ctx context.Context,
	hottestRanges *[]replicaWithStats,
	localDesc *roachpb.StoreDescriptor,
	allStoresList storepool.StoreList,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	options *allocatorimpl.QPSScorerOptions,
) (replicaWithStats, roachpb.ReplicaDescriptor, []replicaWithStats) {
	// NB: Don't switch over to the new locality-aware lease transfer scheme until
	// the cluster version is finalized.
	if !sr.st.Version.IsActive(ctx, clusterversion.EnableNewStoreRebalancer) {
		log.Infof(
			ctx, "cluster version has not been finalized; using pre-22.1 load-based lease transfer scheme",
		)

		// We manually compute the cluster level under/over-fullness thresholds
		// since the deprecated rebalance logic doesn't care about equivalence
		// classes.
		qpsMinThreshold := allocatorimpl.UnderfullQPSThreshold(options, allStoresList.CandidateQueriesPerSecond.Mean)
		qpsMaxThreshold := allocatorimpl.OverfullQPSThreshold(options, allStoresList.CandidateQueriesPerSecond.Mean)
		return sr.deprecatedChooseLeaseToTransfer(
			ctx, hottestRanges, localDesc, allStoresList,
			storeMap, qpsMinThreshold, qpsMaxThreshold,
		)
	}

	var considerForRebalance []replicaWithStats
	now := sr.rq.store.Clock().NowAsClockTimestamp()
	for {
		if len(*hottestRanges) == 0 {
			return replicaWithStats{}, roachpb.ReplicaDescriptor{}, considerForRebalance
		}
		replWithStats := (*hottestRanges)[0]
		*hottestRanges = (*hottestRanges)[1:]

		// We're all out of replicas.
		if replWithStats.repl == nil {
			return replicaWithStats{}, roachpb.ReplicaDescriptor{}, considerForRebalance
		}

		if !replWithStats.repl.OwnsValidLease(ctx, now) {
			log.VEventf(ctx, 3, "store doesn't own the lease for r%d", replWithStats.repl.RangeID)
			continue
		}

		// Don't bother moving leases whose QPS is below some small fraction of the
		// store's QPS. It's just unnecessary churn with no benefit to move leases
		// responsible for, for example, 1 qps on a store with 5000 qps.
		const minQPSFraction = .001
		if replWithStats.qps < localDesc.Capacity.QueriesPerSecond*minQPSFraction {
			log.VEventf(ctx, 3, "r%d's %.2f qps is too little to matter relative to s%d's %.2f total qps",
				replWithStats.repl.RangeID, replWithStats.qps, localDesc.StoreID, localDesc.Capacity.QueriesPerSecond)
			continue
		}

		desc, conf := replWithStats.repl.DescAndSpanConfig()
		log.VEventf(ctx, 3, "considering lease transfer for r%d with %.2f qps",
			desc.RangeID, replWithStats.qps)

		// Check all the other voting replicas in order of increasing qps.
		// Learners or non-voters aren't allowed to become leaseholders or raft
		// leaders, so only consider the `Voter` replicas.
		candidates := desc.Replicas().DeepCopy().VoterDescriptors()

		// Only consider replicas that are not lagging behind the leader in order to
		// avoid hurting QPS in the short term. This is a stronger check than what
		// `TransferLeaseTarget` performs (it only excludes replicas that are
		// waiting for a snapshot).
		candidates = allocatorimpl.FilterBehindReplicas(ctx, sr.getRaftStatusFn(replWithStats.repl), candidates)

		candidate := sr.rq.allocator.TransferLeaseTarget(
			ctx,
			conf,
			candidates,
			replWithStats.repl,
			replWithStats.repl.leaseholderStats,
			true, /* forceDecisionWithoutStats */
			allocator.TransferLeaseOptions{
				Goal:             allocator.QPSConvergence,
				ExcludeLeaseRepl: false,
			},
		)

		if candidate == (roachpb.ReplicaDescriptor{}) {
			log.VEventf(
				ctx,
				3,
				"could not find a better lease transfer target for r%d; considering replica rebalance instead",
				desc.RangeID,
			)
			considerForRebalance = append(considerForRebalance, replWithStats)
			continue
		}

		filteredStoreList := allStoresList.ExcludeInvalid(conf.Constraints)
		filteredStoreList = allStoresList.ExcludeInvalid(conf.VoterConstraints)
		if sr.rq.allocator.FollowTheWorkloadPrefersLocal(
			ctx,
			filteredStoreList,
			*localDesc,
			candidate.StoreID,
			candidates,
			replWithStats.repl.leaseholderStats,
		) {
			log.VEventf(
				ctx, 3, "r%d is on s%d due to follow-the-workload; considering replica rebalance instead",
				desc.RangeID, localDesc.StoreID,
			)
			considerForRebalance = append(considerForRebalance, replWithStats)
			continue
		}
		if targetStore, ok := storeMap[candidate.StoreID]; ok {
			log.VEventf(
				ctx,
				1,
				"transferring lease for r%d (qps=%.2f) to store s%d (qps=%.2f) from local store s%d (qps=%.2f)",
				desc.RangeID,
				replWithStats.qps,
				targetStore.StoreID,
				targetStore.Capacity.QueriesPerSecond,
				localDesc.StoreID,
				localDesc.Capacity.QueriesPerSecond,
			)
		}
		return replWithStats, candidate, considerForRebalance
	}
}

// rangeRebalanceContext represents a snapshot of a replicas's state along with
// the state of the cluster during the StoreRebalancer's attempt to rebalance it
// based on QPS.
type rangeRebalanceContext struct {
	replWithStats replicaWithStats
	rangeDesc     *roachpb.RangeDescriptor
	conf          roachpb.SpanConfig
}

func (sr *StoreRebalancer) chooseRangeToRebalance(
	ctx context.Context,
	hottestRanges *[]replicaWithStats,
	localDesc *roachpb.StoreDescriptor,
	allStoresList storepool.StoreList,
	options *allocatorimpl.QPSScorerOptions,
) (
	replWithStats replicaWithStats,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
	chgs roachpb.ReplicationChanges,
) {
	// NB: Don't switch over to the locality aware rebalancer until the cluster
	// version is finalized.
	if !sr.st.Version.IsActive(ctx, clusterversion.EnableNewStoreRebalancer) {
		log.Infof(
			ctx, "cluster version has not been finalized; using pre-22.1 load-based rebalancing scheme",
		)

		// We manually compute the cluster level under/over-fullness thresholds
		// since the deprecated rebalance logic doesn't care about equivalence
		// classes.
		qpsMinThreshold := allocatorimpl.UnderfullQPSThreshold(
			options, allStoresList.CandidateQueriesPerSecond.Mean,
		)
		qpsMaxThreshold := allocatorimpl.OverfullQPSThreshold(
			options, allStoresList.CandidateQueriesPerSecond.Mean,
		)
		replWithStats, voterTargets, nonVoterTargets := sr.deprecatedChooseRangeToRebalance(ctx, hottestRanges, localDesc, allStoresList, allStoresList.ToMap(), qpsMinThreshold, qpsMaxThreshold)
		// TODO(kvoli): implement this in the function above.
		chgs := roachpb.ReplicationChanges{}
		return replWithStats, voterTargets, nonVoterTargets, chgs
	}

	now := sr.rq.store.Clock().NowAsClockTimestamp()
	for {
		if len(*hottestRanges) == 0 {
			return replicaWithStats{}, nil, nil, roachpb.ReplicationChanges{}
		}
		replWithStats := (*hottestRanges)[0]
		*hottestRanges = (*hottestRanges)[1:]

		if replWithStats.repl == nil {
			return replicaWithStats{}, nil, nil, roachpb.ReplicationChanges{}
		}

		// Don't bother moving ranges whose QPS is below some small fraction of the
		// store's QPS. It's just unnecessary churn with no benefit to move ranges
		// responsible for, for example, 1 qps on a store with 5000 qps.
		const minQPSFraction = .001
		if replWithStats.qps < localDesc.Capacity.QueriesPerSecond*minQPSFraction {
			log.VEventf(
				ctx,
				5,
				"r%d's %.2f qps is too little to matter relative to s%d's %.2f total qps",
				replWithStats.repl.RangeID,
				replWithStats.qps,
				localDesc.StoreID,
				localDesc.Capacity.QueriesPerSecond,
			)
			continue
		}

		rangeDesc, conf := replWithStats.repl.DescAndSpanConfig()
		clusterNodes := sr.rq.allocator.StorePool.ClusterNodeCount()
		numDesiredVoters := allocatorimpl.GetNeededVoters(conf.GetNumVoters(), clusterNodes)
		numDesiredNonVoters := allocatorimpl.GetNeededNonVoters(numDesiredVoters, int(conf.GetNumNonVoters()), clusterNodes)
		if expected, actual := numDesiredVoters, len(rangeDesc.Replicas().VoterDescriptors()); expected != actual {
			log.VEventf(
				ctx,
				3,
				"r%d is either over or under replicated (expected %d voters, found %d); ignoring",
				rangeDesc.RangeID,
				expected,
				actual,
			)
			continue
		}
		if expected, actual := numDesiredNonVoters, len(rangeDesc.Replicas().NonVoterDescriptors()); expected != actual {
			log.VEventf(
				ctx,
				3,
				"r%d is either over or under replicated (expected %d non-voters, found %d); ignoring",
				rangeDesc.RangeID,
				expected,
				actual,
			)
			continue
		}
		rebalanceCtx := rangeRebalanceContext{
			replWithStats: replWithStats,
			rangeDesc:     rangeDesc,
			conf:          conf,
		}

		// We ascribe the leaseholder's QPS to every follower replica. The store
		// rebalancer first attempts to transfer the leases of its hot ranges away
		// in `chooseLeaseToTransfer`. If it cannot move enough leases away to bring
		// down the store's QPS below the cluster-level overfullness threshold, it
		// moves on to rebalancing replicas. In other words, for every hot range on
		// the store, the StoreRebalancer first tries moving the load away to one of
		// its existing replicas but then tries to reconfigure the range (i.e. move
		// the range to a different set of stores) to _then_ hopefully succeed in
		// moving the lease away to another replica.
		//
		// Thus, we ideally want to base our replica rebalancing on the assumption
		// that all of the load from the leaseholder's replica is going to shift to
		// the new store that we end up rebalancing to.
		options.QPSPerReplica = replWithStats.qps

		if !replWithStats.repl.OwnsValidLease(ctx, now) {
			log.VEventf(ctx, 3, "store doesn't own the lease for r%d", replWithStats.repl.RangeID)
			continue
		}

		log.VEventf(
			ctx,
			3,
			"considering replica rebalance for r%d with %.2f qps",
			replWithStats.repl.GetRangeID(),
			replWithStats.qps,
		)

		targetVoterRepls, targetNonVoterRepls, foundRebalance, chgs := sr.getRebalanceTargetsBasedOnQPS(
			ctx,
			rebalanceCtx,
			options,
		)

		if !foundRebalance {
			// Bail if there are no stores that are better for the existing replicas.
			// If the range needs a lease transfer to enable better load distribution,
			// it will be handled by the logic in `chooseLeaseToTransfer()`.
			log.VEventf(ctx, 3, "could not find rebalance opportunities for r%d", replWithStats.repl.RangeID)
			continue
		}

		return replWithStats,
			roachpb.MakeReplicaSet(targetVoterRepls).ReplicationTargets(),
			roachpb.MakeReplicaSet(targetNonVoterRepls).ReplicationTargets(),
			chgs
	}
}

// getRebalanceTargetsBasedOnQPS returns a list of rebalance targets for
// voting and non-voting replicas on the range that match the relevant
// constraints on the range and would further the goal of balancing the QPS on
// the stores in this cluster.
func (sr *StoreRebalancer) getRebalanceTargetsBasedOnQPS(
	ctx context.Context, rbCtx rangeRebalanceContext, options allocatorimpl.ScorerOptions,
) (
	finalVoterTargets, finalNonVoterTargets []roachpb.ReplicaDescriptor,
	foundRebalance bool,
	chgs roachpb.ReplicationChanges,
) {
	finalVoterTargets = rbCtx.rangeDesc.Replicas().VoterDescriptors()
	finalNonVoterTargets = rbCtx.rangeDesc.Replicas().NonVoterDescriptors()

	// NB: We attempt to rebalance N times for N replicas as we may want to
	// replace all of them (they could all be on suboptimal stores).
	// TODO(aayush): Figure out a way to plumb the `details` here into
	// `AdminRelocateRange` so that these decisions show up in system.rangelog
	add, remove, _, shouldRebalance := sr.rq.allocator.RebalanceTarget(
		ctx,
		rbCtx.conf,
		rbCtx.replWithStats.repl.RaftStatus(),
		finalVoterTargets,
		finalNonVoterTargets,
		rangeUsageInfoForRepl(rbCtx.replWithStats.repl),
		storepool.StoreFilterSuspect,
		allocatorimpl.VoterTarget,
		options,
	)
	if !shouldRebalance {
		log.VEventf(
			ctx,
			3,
			"no more rebalancing opportunities for r%d voters that improve QPS balance",
			rbCtx.rangeDesc.RangeID,
		)
	} else {
		// Record the fact that we found at least one rebalance opportunity.
		log.VEventf(
			ctx,
			3,
			"rebalancing voter (qps=%.2f) for r%d on %v to %v in order to improve QPS balance",
			rbCtx.replWithStats.qps,
			rbCtx.rangeDesc.RangeID,
			remove,
			add,
		)

		chg, _, err := replicationChangesForRebalance(ctx, rbCtx.rangeDesc, len(finalVoterTargets), add,
			remove, allocatorimpl.VoterTarget)
		if err != nil {
			return finalVoterTargets, finalNonVoterTargets, false, roachpb.ReplicationChanges{}
		}
		chgs = chg
		foundRebalance = true
	}

	afterVoters := make([]roachpb.ReplicaDescriptor, 0, len(finalVoterTargets))
	afterNonVoters := make([]roachpb.ReplicaDescriptor, 0, len(finalNonVoterTargets))
	for _, voter := range finalVoterTargets {
		if voter.StoreID == remove.StoreID {
			afterVoters = append(
				afterVoters, roachpb.ReplicaDescriptor{
					StoreID: add.StoreID,
					NodeID:  add.NodeID,
				})
		} else {
			afterVoters = append(afterVoters, voter)
		}
	}
	// Voters are allowed to relocate to stores that have non-voters, which may
	// displace them.
	for _, nonVoter := range finalNonVoterTargets {
		if nonVoter.StoreID == add.StoreID {
			afterNonVoters = append(afterNonVoters, roachpb.ReplicaDescriptor{
				StoreID: remove.StoreID,
				NodeID:  remove.NodeID,
			})
		} else {
			afterNonVoters = append(afterNonVoters, nonVoter)
		}
	}
	// Pretend that we've executed upon this rebalancing decision.
	finalVoterTargets = afterVoters
	finalNonVoterTargets = afterNonVoters

	var newNonVoters []roachpb.ReplicaDescriptor
	for _, nonVoter := range finalNonVoterTargets {
		if nonVoter.StoreID == remove.StoreID {
			newNonVoters = append(
				newNonVoters, roachpb.ReplicaDescriptor{
					StoreID: add.StoreID,
					NodeID:  add.NodeID,
				})
		} else {
			newNonVoters = append(newNonVoters, nonVoter)
		}
	}
	// Pretend that we've executed upon this rebalancing decision.
	finalNonVoterTargets = newNonVoters

	return finalVoterTargets, finalNonVoterTargets, foundRebalance, chgs
}

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from checkInterval.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.75 + 0.5*rand.Float64()))
}
