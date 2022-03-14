// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cputime

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestInternalDuration tests internal APIs measuring goroutine running time.
func TestInternalDuration(t *testing.T) {
	for _, work := range []string{"busy", "lazy"} {
		t.Run(fmt.Sprintf("loop=%s", work), func(t *testing.T) {
			tstart := time.Now()
			var totalnanos int64

			var wg sync.WaitGroup
			for g := 0; g < 10; g++ {
				wg.Add(1)
				go func(wg *sync.WaitGroup, total *int64) {
					start := grunningnanos()
					if work == "busy" {
						TestingBusyFn()
					} else {
						TestingLazyFn()
					}
					end := grunningnanos()

					atomic.AddInt64(total, end-start)
					wg.Done()
				}(&wg, &totalnanos)
			}
			wg.Wait()

			walltime := time.Since(tstart)
			cputime := time.Duration(totalnanos)
			mult := float64(cputime.Nanoseconds()) / float64(walltime.Nanoseconds())

			if work == "busy" {
				minexp := float64(runtime.GOMAXPROCS(-1)) - 1
				assert.Greaterf(t, mult, minexp,
					"expected multiplier > %f, got %f", minexp, mult)
			} else {
				maxexp := float64(0.1)
				assert.Lessf(t, mult, maxexp,
					"expected approximately zero multiplier, got %f", mult)
			}
		})
	}
}

// BenchmarkMetricNanos measures how costly it is to read the current
// goroutine's running time when going through the exported runtime metric.
func BenchmarkMetricNanos(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_ = metricnanos()
	}
}

// BenchmarkGRunningNanos measures how costly it is to read the current
// goroutine's running time when going through an internal runtime API.
func BenchmarkGRunningNanos(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_ = grunningnanos()
	}
}

func TestingBusyFn() {
	j := 1
	for i := 0; i < 5000000000; i++ {
		j = j - i + 42
	}
}

func TestingLazyFn() {
	time.Sleep(time.Millisecond * 100)
}
