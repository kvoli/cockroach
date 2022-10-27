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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/replicastats"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// problems:
// - many things use replicastats directly and there is no uniformity in what
//   uses what
//   sol: use only one, allocator pkg struct that represents the load of a
//   replica
//
// - replica stats is not configurable for decay and does not have an interface
//   interface e.g. we want to add in cpu to be tracked, however we want to
//   modify things underneath for period, decaying and so on.
//   sol: create an interface on the accounting side, create an interface on
//   the using side.
//
// - adding more stats uses more memory, having a different struct per stat is
//   expensive
//   sol: consolidate stats into one struct, one mutex.
//
// - per-locality counts applies specifically to qps and nearly nothing else.
//   sol: split this out explicity from the API
//
// - ui hot ranges is coupled to replica rankings, which in turn cals the
// XPerSecond methods directly for load.
//
// sol
//    (1) make a single struct (or interface) on the allocator side that will
//        contain everything. i.e. update everything to use RangeUsageInfo.
//    (2) Add support for generating RangeUsageInfo from replica load.
//    (3) wrap all load accounting into a single interface. which may have
//        different accounting beneath it.
//
// most important is to separate collection i.e from updating
// need to separate collection from updating
//
// record
//   bumping counters e.g. bump writekeys counter += N
// collect and store with different aggregating uses
//   collect from load counters
//   collect from local state
//   collect from gossip
// query aggregations and use them to make decisions
//   averageX, medianX, ...
//   rankings
//   create capacity
//   use as dimension in allocator
// update collected aggregations on actions (applies to aggregations)
//   split, merge, reset
//
// balancing
//   check balance
//   find action
//
// (1) refactor allocator methods to take an arbitrary dimension:
//   - [ ] StoreRebalancer
//   - [ ] Allocator Methods
//   - [ ] QPSScorerOptions
//
// (2) refactor replicastats, replica rankings, capacity  range usage info to
// be decoupled.
//   -  [ ] ReplicaStats
//   -  [ ] ReplicaRankings
//   -  [ ] Capacity/StorePool
//   -  [ ] RangeUsageInfo
//
// 1d hard to refactor parts:
// - store_rebalancer RebalanceRanges
// - store_rebalancer chooseLeaseToTransfer

// ReplicaLoad tracks a sliding window of throughput on a replica. By default,
// there are 6, 5 minute sliding windows.
type ReplicaLoad struct {
	batchRequests *replicastats.ReplicaStats
	requests      *replicastats.ReplicaStats
	writeKeys     *replicastats.ReplicaStats
	readKeys      *replicastats.ReplicaStats
	writeBytes    *replicastats.ReplicaStats
	readBytes     *replicastats.ReplicaStats
}

// NewReplicaLoad returns a new ReplicaLoad, which may be used to track the
// request throughput of a replica.
func NewReplicaLoad(clock *hlc.Clock, getNodeLocality replicastats.LocalityOracle) *ReplicaLoad {
	// NB: We only wish to record the locality of a request for QPS, where it
	// as only follow-the-workload lease transfers use this per-locality
	// request count. Maintaining more than one bucket for client requests
	// increases the memory footprint O(localities).
	return &ReplicaLoad{
		batchRequests: replicastats.NewReplicaStats(clock, getNodeLocality),
		requests:      replicastats.NewReplicaStats(clock, nil),
		writeKeys:     replicastats.NewReplicaStats(clock, nil),
		readKeys:      replicastats.NewReplicaStats(clock, nil),
		writeBytes:    replicastats.NewReplicaStats(clock, nil),
		readBytes:     replicastats.NewReplicaStats(clock, nil),
	}
}

// split will distribute the load in the calling struct, evenly between itself
// and other.
func (rl *ReplicaLoad) split(other *ReplicaLoad) {
	rl.batchRequests.SplitRequestCounts(other.batchRequests)
	rl.requests.SplitRequestCounts(other.requests)
	rl.writeKeys.SplitRequestCounts(other.writeKeys)
	rl.readKeys.SplitRequestCounts(other.readKeys)
	rl.writeBytes.SplitRequestCounts(other.writeBytes)
	rl.readBytes.SplitRequestCounts(other.readBytes)
}

// merge will combine the tracked load in other, into the calling struct.
func (rl *ReplicaLoad) merge(other *ReplicaLoad) {
	rl.batchRequests.MergeRequestCounts(other.batchRequests)
	rl.requests.MergeRequestCounts(other.requests)
	rl.writeKeys.MergeRequestCounts(other.writeKeys)
	rl.readKeys.MergeRequestCounts(other.readKeys)
	rl.writeBytes.MergeRequestCounts(other.writeBytes)
	rl.readBytes.MergeRequestCounts(other.readBytes)
}

// reset will clear all recorded history.
func (rl *ReplicaLoad) reset() {
	rl.batchRequests.ResetRequestCounts()
	rl.requests.ResetRequestCounts()
	rl.writeKeys.ResetRequestCounts()
	rl.readKeys.ResetRequestCounts()
	rl.writeBytes.ResetRequestCounts()
	rl.readBytes.ResetRequestCounts()
}
