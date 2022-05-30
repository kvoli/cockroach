// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tick

import "time"

const tickUnit = time.Millisecond

// Tick is a measure of milliseconds since the start of a simulation.
type Tick int64

// GoTime converts Tick to Go time.Time.
func (t Tick) GoTime(start time.Time) time.Time {
	return start.Add(tickUnit * time.Duration(t))
}

// UnixNano converts Tick to UnixNano.
func (t Tick) UnixNano(start time.Time) int64 {
	return t.GoTime(start).UnixNano()
}

// FromGoTime converts now to a Tick.
func FromGoTime(start time.Time, now time.Time) Tick {
	return Tick(now.Sub(start).Milliseconds())
}
