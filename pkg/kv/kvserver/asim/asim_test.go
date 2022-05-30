// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestRunAllocatorSimulator(t *testing.T) {
	ctx := context.Background()
	rwg := make([]workload.Generator, 1)
	rwg[0] = &workload.RandomGenerator{}
	start := state.TestingStartTime()
	end := start.Add(1000 * time.Second)
	interval := 10 * time.Second

	exchange := state.NewFixedDelayExhange(start, interval, interval)
	changer := state.NewReplicaChanger()
	s := state.LoadConfig(state.ComplexConfig)

	sim := asim.NewSimulator(start, end, interval, rwg, s, exchange, changer)
	sim.RunSim(ctx)
}

// TestAllocatorSimulatorSpeed tests that the simulation runs at a rate of at
// reast 5 simulated minutes per wall clock second (1:500) for a large 64 node
// cluster, with between 0 and 6400 replicas on each node.
func TestAllocatorSimulatorSpeed(t *testing.T) {
	ctx := context.Background()
	rwg := make([]workload.Generator, 1)
	rwg[0] = &workload.RandomGenerator{}
	start := state.TestingStartTime()
	end := start.Add(20 * time.Minute)
	interval := 10 * time.Second

	exchange := state.NewFixedDelayExhange(start, interval, interval)
	changer := state.NewReplicaChanger()
	replicaCounts := make(map[state.StoreID]int)
	for i := 0; i < 64; i++ {
		replicaCounts[state.StoreID(i+1)] = i * 100
	}
	s, _ := state.NewTestStateReplCounts(replicaCounts)
	sim := asim.NewSimulator(start, end, interval, rwg, s, exchange, changer)

	startTime := timeutil.Now()
	sim.RunSim(ctx)
	runDuration := timeutil.Since(startTime)
	require.Less(t, runDuration, 4*time.Second)
}
