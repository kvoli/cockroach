// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package state

import (
	"bytes"
	"fmt"
)

type LoadDimension int

const (
	RangeCountDimension LoadDimension = iota
	LeaseCountDimension
	QueriesDimension
	WriteKeysDimension
	StorageDimension
)

// LoadDimensionNames contains a mapping of a load dimension, to a human
// readable string.
var LoadDimensionNames = map[LoadDimension]string{
	RangeCountDimension: "range-count",
	LeaseCountDimension: "lease-count",
	QueriesDimension:    "queries-per-second",
	WriteKeysDimension:  "write-keys-per-second",
	StorageDimension:    "disk-storage",
}

// Dimension Datastructures
//
// Container of Ranges
// - HottestK(Dimension)
//
// Range
// - Value(LoadDimension)
//
// Container of Stores
// - Mean(LoadDimension)
// - Filter()
// - FromIDs()
//
// Store
// - Value(LoadDimension)
//
// Options
// - RebalanceThreshold()
// - MinRequiredDiff()
// - ValuePerReplica
//
// DimensionContainer...
//
// range (val) / replica [local]
// store (val) [storepoo]
// store list (means) [storepool]
// threshold (max,min,%) [setting]
// min required diff [setting]
//
// It would be nice to record statistics with tags, per replica.
// - cpu: leaseholder (store)
// - cpu: replica (raft)
//
//
// supporting 2 dimensions:
// - impact of each action needs to be clear
// -
//
type DimensionContainer interface {
	Dimension(LoadDimension) float64
	Filter(...LoadDimension) DimensionContainer
	LessOrEqual(other DimensionContainer) bool
	Sub(other DimensionContainer) DimensionContainer
	Add(other DimensionContainer) DimensionContainer
	String() string
}

type StaticDimensionContainer struct {
	vals [5]float64
	// TODO(kvoli): make this a bitset.
	set [5]bool
}

func (s StaticDimensionContainer) Dimension(dimension LoadDimension) float64 {
	if int(dimension) > len(s.vals) || dimension < 0 {
		panic("Unkown load dimension tried to be accessed")
	}
	return s.vals[dimension]
}

// TODO(kvoli): fill this in
func (s StaticDimensionContainer) Filter(dimensions ...LoadDimension) DimensionContainer {
	filtered := s
	for _, dim := range dimensions {
		s.set[dim] = true
	}
	return filtered
}

func (s StaticDimensionContainer) LessOrEqual(other DimensionContainer) bool {
	for dim := range s.vals {
		if !s.set[dim] {
			continue
		}
		if s.Dimension(LoadDimension(dim)) > other.Dimension(LoadDimension(dim)) {
			return false
		}
	}
	return true
}

func (s StaticDimensionContainer) Sub(other DimensionContainer) DimensionContainer {
	return s.bimap(other, func(a, b float64) float64 { return a - b })
}

func (s StaticDimensionContainer) Add(other DimensionContainer) DimensionContainer {
	return s.bimap(other, func(a, b float64) float64 { return a + b })
}

func (s StaticDimensionContainer) bimap(
	other DimensionContainer, op func(a, b float64) float64,
) DimensionContainer {
	mapped := StaticDimensionContainer{}
	for i := range s.vals {
		mapped.vals[i] = op(s.Dimension(LoadDimension(i)), other.Dimension(LoadDimension(i)))
	}
	return mapped
}

func (s StaticDimensionContainer) String() string {
	var buf bytes.Buffer
	fmt.Fprint(&buf, "(")
	for i, val := range s.vals {
		if s.set(1 << i) {
			continue
		}
		fmt.Fprintf(&buf, "%s=%.2f ", LoadDimensionNames[LoadDimension(i)], val)
	}
	fmt.Fprint(&buf, ")")
	return buf.String()
}
