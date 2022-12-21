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

// goal
//   - balance process CPU time across the cluster
// challenges
//   - multiple stores per process
//   - disconnect between action CPU time and process CPU time
// design
//   - gossip struct like below
//     type GossipCpu struct {
//    	 process     float64
//       allStores   float64
//     }
//   - each store uses the process CPU from the store capacity to calculate
//     under/overfull and pick targets.
//   - when estimating the impact we use the formula
//     (impact / localStore) *       // % of local store this action impact represents
//     (localStore / allStores) *    // % of all stores this store represents
//     process                       // total process cpu time
//     which should be equivalently (impact / allStores) * process
//     e.g.
//        stores per node = 1
//          impact = 10 allStores = 100 process = 300
//          estimate = (10 / 100) * (100 / 100) * 300
//                   = 1/10 * 300
//                   = 30

//        stores per node = 2
//          impact = 10 allStores = 100 process = 300
//          estimate = (10 / 25) * (25 / 100) * 300
//                   = 4/10 * 1/4 * 300
//                   = 30
//
//
//   - for now lets assume the number of stores is always 1 per node.
//   - values then required that are not available are
//       allStores: sum of the replica cpu time across all stores on a node (simplifying, this is just the 1 store)
//       process  : the process average cpu time per second over the last N period
// plan
// (1) grab process cpu from runtime.go and pass interface ref to the store.go
// (2) gossip process cpu
// (3) update logic on rebalance impact to account for process cpu as mentioned above
//
// layout
// * principled here on simulation and short as reasonable
//   getting the stat (stores, process)
//   - each server maintains a cpu stat sampler, this is smoothed over 5 minutes
//   - each store maintains a runtime cpu variable that is updated periodically by the stat sampler
//   - when calculating the capacity we call the store's runtime cpu variable to populate
//   - the capacity is gossiped as usual and recieved on other's storepools
//   using the stat
//   - scale all actions as = impact * (store / process)
//     - update local storepool after
//     - comparison in store rebalancer (worthwhile, logging, etc)
//     - creating rebalance impact in load scorer options
//   - set values for thresholds
//
//  simulator
//  generating the stat -> gossip
//    - store load struct can maintain this
//  gossip -> using the stat
//    - nothing to do here
//
//
// data:
// node_cpu
// - required for
//   - creating store capacity (1:node_stores)
//   - calculating scale (1:node_stores)
// sum(store_replica_cpu)
//   - calculating scale (1:node_stores)
// each node/server should have 1 tracker for node_cpu and store_replica_cpu.
// 
// node (store_replica_cpu)       <- [store_1,...,store_n]
// node (process_cpu)             <- runtime
// 
// store (node_cpu)               <- node
// store (sum(store_replica_cpu)) <- node
//
//
// 
