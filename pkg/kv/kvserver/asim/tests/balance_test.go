// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

// vars
//   +num_stores         range 
//   +multi_store        count 
//   +multi_locality     perm  
//   +range_zone_config  perm  
//   +init_splits        dist  
//   +init_repl_loc      dist  
//   +init_lh_loc        dist
//   +workload_dist      dist  
//   +workload_rate      const
//
//   +gossip_delay       range
//   +qps_thresh         range
//   +range_thresh       range
//   +size_split_thresh  range
//   +load_split_thresh  range
//
//   +lb_rebalance_mode  option
//   +repl_q_transfers   option
//   +repl_q_enabled     option
//   +split_q_enabled    option
//
// assertions - macro (cluster)
//   +steady_state_before   time
//   +balances_localities   ...
//   -satisfies_preferences ... 
// assertions - micro (specific range)
//   -expected_changes      ...
//   -expected_range_sel    ...

func TestReplicateQueueBalancesLeases(t *testing.T) {}

func TestReplicateQueueBalancesReplicas(t *testing.T) {}

func TestStoreRebalancerBalancesQPS(t *testing.T) {}

func TestAllocationSystemBalances(t *testing.T) {}
