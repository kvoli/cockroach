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

import (
	"context"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const defaultRuntimeCPUPeriod = 1 * time.Second
const defaultRuntimeCPUTrackedSeconds = 5 * 60 // 5 mins

func StartSampler(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "kvserver-runtime-tracker", func(ctx context.Context) {
		ticker := time.NewTicker(defaultRuntimeCPUPeriod)
		defer ticker.Stop()

		s := newSampler()

		for {
			select {
			case <-ctx.Done():
				return
			case <-stopper.ShouldQuiesce():
				return
			case <-ticker.C:
				s.sampleOnTick(ctx, defaultRuntimeCPUPeriod)
			}
		}
	})
}

type sampler struct {
	mu struct {
		syncutil.Mutex
		first, second int64
		tracker       *runtimeTracker
	}
}

func newSampler() *sampler {
	s := &sampler{}
	s.mu.tracker = newRuntimeTracker(defaultRuntimeCPUTrackedSeconds)
	return s
}

func (s *sampler) sampleOnTick(ctx context.Context, period time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	latest, err := sample(ctx)
	if err != nil {
		log.Infof(ctx, "failed to get cpu time sample")
	}

	s.mu.second = s.mu.first
	s.mu.first = latest
	instant := s.mu.first - s.mu.second

	s.mu.tracker.recordCPU(instant, period)
	rate := s.mu.tracker.movingAverage.Value()

	log.Infof(ctx, "sampled cpu: rate=%f instant=%d period=%s", rate, instant, period)

	globallyRegisteredCallbacks.mu.Lock()
	defer globallyRegisteredCallbacks.mu.Unlock()
	cbs := globallyRegisteredCallbacks.mu.callbacks
	for i := range cbs {
		cbs[i].cb(rate)
	}
}

func sample(ctx context.Context) (int64, error) {
	// User and System cpu time are in milliseconds, convert to nanoseconds.
	utimeMillis, stimeMillis, err := status.GetCPUTime(ctx)
	utime := utimeMillis * 1e6
	stime := stimeMillis * 1e6
	return utime + stime, err
}

type runtimeTracker struct {
	movingAverage ewma.MovingAverage
}

func newRuntimeTracker(durationSeconds int) *runtimeTracker {
	return &runtimeTracker{
		movingAverage: ewma.NewMovingAverage(float64(durationSeconds) / 2),
	}
}

func (rt *runtimeTracker) recordCPU(nanos int64, period time.Duration) {
	rated := float64(nanos) / period.Seconds()
	rt.movingAverage.Add(rated)
}
