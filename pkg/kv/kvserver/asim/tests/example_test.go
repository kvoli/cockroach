// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/montanaflynn/stats"
	"github.com/stretchr/testify/require"
)

type SimulationAssertion interface {
	Assert(context.Context, [][]metrics.StoreMetrics) (bool, string)
	String() string
}

type steadyStateAssertion struct {
	ticks     int
	stat      string
	threshold float64
}

//
func (sa steadyStateAssertion) Assert(
	ctx context.Context, m [][]metrics.StoreMetrics,
) (bool, string) {
	ticks := len(m)
	if sa.ticks > ticks {
		log.Infof(ctx, "no history to run assertions against")
		return true, ""
	}

	ts := metrics.MakeTS(m)
	statTs := ts[sa.stat]

	for i, storeStats := range statTs {
		trimmedStoreStats := storeStats[ticks-sa.ticks-1:]
		mean, _ := stats.Mean(trimmedStoreStats)
		max, _ := stats.Max(trimmedStoreStats)
		min, _ := stats.Min(trimmedStoreStats)

		maxMean := math.Abs(max/mean - 1)
		minMean := math.Abs(min/mean - 1)

		if maxMean > sa.threshold || minMean > sa.threshold {
			return false, fmt.Sprintf(
				"steady state assertion failed: stat=%s store=%d minmax/mean=[%.2f, %.2f] threshold=%.2f",
				sa.stat, i+1, maxMean, minMean, sa.threshold)
		}
	}

	return true, fmt.Sprintf("steady state assertion passed: stat=%s threhsold=%f",
		sa.stat, sa.threshold)
}

func (sa steadyStateAssertion) String() string {
	return fmt.Sprintf("steady(stat=%s threshold=%.2f ticks=%d)", sa.stat, sa.threshold, sa.ticks)
}

type balanceAssertion struct {
	ticks     int
	stat      string
	threshold float64
}

func (ba balanceAssertion) String() string {
	return fmt.Sprintf("balance(stat=%s threshold=%.2f ticks=%d)", ba.stat, ba.threshold, ba.ticks)
}

func (ba balanceAssertion) Assert(ctx context.Context, m [][]metrics.StoreMetrics) (bool, string) {
	ticks := len(m)
	if ba.ticks > ticks {
		log.Infof(ctx, "no history to run assertions against")
		return true, ""
	}

	ts := metrics.MakeTS(m)
	statTs := metrics.Transpose(ts[ba.stat])


	for tick := 0; tick < ba.ticks && tick < ticks; tick++ {
		tickStats := statTs[ticks-tick-1]
		mean, _ := stats.Mean(tickStats)
		max, _ := stats.Max(tickStats)
		maxMeanRatio := max / mean
		log.VInfof(ctx, 2, "balance assertion: stat=%s, max/mean=%.2f, threshold=%.2f raw=%v",
			ba.stat, maxMeanRatio, ba.threshold, tickStats)
		if maxMeanRatio > ba.threshold {
			return false, fmt.Sprintf(
				"balance assertion failed: stat=%s max/mean=%.2f threshold=%.2f",
				ba.stat, maxMeanRatio, ba.threshold)
		}
	}

	return true, fmt.Sprintf("balance assertion passed: stat=%s max/mean <= threshold=%f", ba.stat, ba.threshold)
}

func TestSim(t *testing.T) {
	ctx := context.Background()
	samples := 1

	assertions := []balanceAssertion{
		{
			ticks:     30,
			stat:      "qps",
			threshold: 1.15,
		},
		{
			ticks:     30,
			stat:      "leases",
			threshold: 1.15,
		},
	}

	generator := gen.SimulationGenerator{
		Duration: 30 * time.Minute,
		State: gen.BasicState{
			Stores:            7,
			Ranges:            7,
			SkewedPlacement:   true,
			KeySpace:          10000,
			ReplicationFactor: 3,
		},
		Load: gen.BasicLoad{
			RWRatio:      0.95,
			Rate:         7000,
			SkewedAccess: false,
			MinBlockSize: 128,
			MaxBlockSize: 256,
			KeySpace:     10000,
		},
		Settings: gen.StaticSettings{*config.DefaultSimulationSettings()},
	}

	for sample := 0; sample < samples; sample++ {
		seed := rand.Int63()
		t.Run(fmt.Sprintf("iter=%d/seed=%d", sample, seed), func(t *testing.T) {
			simulator := generator.Generate(seed)
			simulator.RunSim(ctx)
			history := simulator.History()

			for _, assertion := range assertions {
				ok, reason := assertion.Assert(ctx, history)
				require.Truef(t, ok, reason)
			}
		})
	}
}
