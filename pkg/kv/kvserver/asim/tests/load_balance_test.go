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
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/guptarohit/asciigraph"
	"github.com/stretchr/testify/require"
)

// TestLoadBalance is a datadriven test for simulating rebalancing using the
// simulator.
//
//   - "gen_load" [rw_ratio=<float>] [rate=<float>] [access_skew=<bool>]
//                [min_block=<int>] [max_block=<int>]
//
//   - "gen_state" [stores=<int>] [ranges=<int>] [placement_skew=<bool>]
//                 [repl_factor=<int>]
//
//   - "assertion" type=<string> stat=<string> ticks=<int> threshold=<float>
//
//   - "setting" [rebalance_mode=<int>] [rebalance_interval=<duration>]
//               [rebalance_qps_threshold=<float>] [split_qps_threshold=<float>]
//               [rebalance_range_threshold=<float>] [gossip_delay=<duration>]
//
//   - "eval" [duration=<int>] [samples=<int>] [seed=<int>]
//
//   - "plot" stat=<string> [sample=<int>] [height=<int>] [width=<int>]
//
func TestLoadBalance(t *testing.T) {
	const keyspace = 10000
	ctx := context.Background()
	dir := datapathutils.TestDataPath(t, "loadbalance")
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		loadGen := gen.BasicLoad{}
		stateGen := gen.BasicState{}
		settingsGen := gen.StaticSettings{*config.DefaultSimulationSettings()}
		assertions := []SimulationAssertion{}
		runs := []*asim.Simulator{}
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "gen_load":
				var rwRatio, rate = 0.0, 0.0
				var minBlock, maxBlock = 1, 1
				var accessSkew bool

				scanIfExists(t, d, "rw_ratio", &rate)
				scanIfExists(t, d, "rate", &rwRatio)
				scanIfExists(t, d, "access_skew", &accessSkew)
				scanIfExists(t, d, "min_block", &minBlock)
				scanIfExists(t, d, "max_block", &maxBlock)

				loadGen.SkewedAccess = accessSkew
				loadGen.KeySpace = keyspace
				loadGen.RWRatio = rwRatio
				loadGen.Rate = rate
				loadGen.MaxBlockSize = maxBlock
				loadGen.MinBlockSize = minBlock
				return ""
			case "gen_state":
				var stores, ranges, replFactor = 3, 1, 3
				var placementSkew bool

				scanIfExists(t, d, "stores", &stores)
				scanIfExists(t, d, "ranges", &ranges)
				scanIfExists(t, d, "repl_factor", &replFactor)
				scanIfExists(t, d, "placement_skew", &placementSkew)

				stateGen.Stores = stores
				stateGen.ReplicationFactor = replFactor
				stateGen.KeySpace = keyspace
				stateGen.Ranges = ranges
				stateGen.SkewedPlacement = placementSkew
				return ""
			case "eval":
				samples := 1
				seed := rand.Int63()
				duration := 30 * time.Minute

				scanIfExists(t, d, "duration", &duration)
				scanIfExists(t, d, "samples", &samples)
				scanIfExists(t, d, "seed", &seed)

				generator := gen.SimulationGenerator{
					Duration: duration,
					State:    stateGen,
					Load:     loadGen,
					Settings: settingsGen,
				}
				seedGen := rand.New(rand.NewSource(seed))
				for sample := 0; sample < samples; sample++ {
					assertionFailures := []string{}
					simulator := generator.Generate(seedGen.Int63())
					simulator.RunSim(ctx)
					history := simulator.History()
					// TODO(kvoli): keep only the history of a run.
					runs = append(runs, simulator)
					for _, assertion := range assertions {
						if ok, reason := assertion.Assert(ctx, history); !ok {
							assertionFailures = append(assertionFailures, reason)
						}
					}
					if len(assertionFailures) > 0 {
						return strings.Join(assertionFailures, "\n")
					}
				}
				return "OK"
			case "assertion":
				var stat string
				var typ string
				var ticks int
				var threshold float64

				scanArg(t, d, "type", &typ)
				scanArg(t, d, "stat", &stat)
				scanArg(t, d, "ticks", &ticks)
				scanArg(t, d, "threshold", &threshold)

				switch typ {
				case "balance":
					assertions = append(assertions, balanceAssertion{
						ticks:     ticks,
						stat:      stat,
						threshold: threshold,
					})
				case "steady":
					assertions = append(assertions, steadyStateAssertion{
						ticks:     ticks,
						stat:      stat,
						threshold: threshold,
					})
				}
				return fmt.Sprintf("assertions=%v", assertions)
			case "setting":
				scanIfExists(t, d, "rebalance_mode", &settingsGen.Settings.LBRebalancingMode)
				scanIfExists(t, d, "rebalance_interval", &settingsGen.Settings.LBRebalancingInterval)
				scanIfExists(t, d, "rebalance_qps_threshold", &settingsGen.Settings.LBRebalanceQPSThreshold)
				scanIfExists(t, d, "split_qps_threshold", &settingsGen.Settings.SplitQPSThreshold)
				scanIfExists(t, d, "rebalance_range_threshold", &settingsGen.Settings.RangeRebalanceThreshold)
				scanIfExists(t, d, "gossip_delay", &settingsGen.Settings.StateExchangeDelay)
				return ""
			case "plot":
				var stat string
				var height, width, sample = 15, 80, 1
				var buf strings.Builder

				scanArg(t, d, "stat", &stat)
				scanArg(t, d, "sample", &sample)
				scanIfExists(t, d, "height", &height)
				scanIfExists(t, d, "width", &width)

				require.GreaterOrEqual(t, len(runs), sample)

				history := runs[sample-1].History()
				ts := metrics.MakeTS(history)
				statTS := ts[stat]
				buf.WriteString("\n")
				buf.WriteString(asciigraph.PlotMany(
					statTS,
					asciigraph.Caption(stat),
					asciigraph.Height(height),
					asciigraph.Width(width),
				))
				buf.WriteString("\n")
				return buf.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

func scanArg(t *testing.T, d *datadriven.TestData, key string, dest interface{}) {
	var tmp string
	var err error
	switch dest := dest.(type) {
	case *time.Duration:
		d.ScanArgs(t, key, &tmp)
		*dest, err = time.ParseDuration(tmp)
		require.NoError(t, err)
	case *float64:
		d.ScanArgs(t, key, &tmp)
		*dest, err = strconv.ParseFloat(tmp, 64)
		require.NoError(t, err)
	case *string, *int, *int64, *uint64, *bool:
		d.ScanArgs(t, key, dest)
	default:
		require.Fail(t, "unsupported type %T", dest)
	}
}

func scanIfExists(t *testing.T, d *datadriven.TestData, key string, dest interface{}) {
	if d.HasArg(key) {
		scanArg(t, d, key, dest)
	}
}
