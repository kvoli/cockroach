// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gen

import (
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
)

type Settings interface {
	Generate(seed int64) config.SimulationSettings
}

type StaticSettings struct {
	Settings config.SimulationSettings
}

func (ss StaticSettings) Generate(seed int64) config.SimulationSettings {
	ret := ss.Settings
	ret.Seed = seed
	return ss.Settings
}

type Load interface {
	Generate(seed int64) []workload.Generator
}

type BasicLoad struct {
	RWRatio      float64
	Rate         float64
	SkewedAccess bool
	MinBlockSize int
	MaxBlockSize int
	KeySpace     int64
}

func (bl BasicLoad) Generate(seed int64) []workload.Generator {
	var keyGen workload.KeyGenerator
	rand := rand.New(rand.NewSource(seed))
	if bl.SkewedAccess {
		keyGen = workload.NewZipfianKeyGen(bl.KeySpace, 1.1, 1, rand)
	} else {
		keyGen = workload.NewUniformKeyGen(bl.KeySpace, rand)
	}

	return []workload.Generator{
		workload.NewRandomGenerator(
			// TODO(kvoli): fix this time being here.
			state.TestingStartTime(),
			seed,
			keyGen,
			bl.Rate,
			bl.RWRatio,
			bl.MaxBlockSize,
			bl.MinBlockSize,
		),
	}
}

type State interface {
	Generate(seed int64) state.State
}

type StaticState struct {
	ClusterInfo state.ClusterInfo
	RangesInfo  state.RangesInfo
}

func (ss StaticState) Generate(seed int64) state.State {
	return state.LoadConfig(ss.ClusterInfo, ss.RangesInfo)
}

type BasicState struct {
	Stores            int
	Ranges            int
	SkewedPlacement   bool
	KeySpace          int
	ReplicationFactor int
}

func (bs BasicState) Generate(seed int64) state.State {
	var s state.State
	if bs.SkewedPlacement {
		s = state.NewTestStateSkewedDistribution(bs.Stores, bs.Ranges, bs.ReplicationFactor, bs.KeySpace)
	} else {
		s = state.NewTestStateEvenDistribution(bs.Stores, bs.Ranges, bs.ReplicationFactor, bs.KeySpace)
	}
	return s
}

type SimulationGenerator struct {
	Duration time.Duration
	State    State
	Load     Load
	Settings Settings
}

func (sg SimulationGenerator) Generate(seed int64) *asim.Simulator {
	settings := sg.Settings.Generate(seed)
	return asim.NewSimulator(
		sg.Duration,
		sg.Load.Generate(seed),
		sg.State.Generate(seed),
		&settings,
		metrics.NewTracker(settings.MetricsInterval),
	)
}
