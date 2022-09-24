// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package split

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/exp/rand"
)

// TestSplitFinderDistributions searches for the number of samples required in
// the finder to select a split key with X%(35) accuracy of the optimal.
//
// ./dev test pkg/kv/kvserver/split -f TestSplitFinderDistribution -v --show-logs
//  === RUN   TestSplitFinderDistributions/span=95/length=uniform/access=zipf
//     finder_test.go:142: sample_size=10 suggested=-1, optimal=528, penalty(/100)=100.00
//     finder_test.go:142: sample_size=20 suggested=-1, optimal=528, penalty(/100)=100.00
//     finder_test.go:149: sample_size=40 suggested=675, optimal=532, penalty(/100)=9.44
// === RUN   TestSplitFinderDistributions/span=50/length=zipf/access=zipf
//     finder_test.go:149: sample_size=10 suggested=79, optimal=421, penalty(/100)=35.12
//     finder_test.go:149: sample_size=20 suggested=63, optimal=403, penalty(/100)=36.61
//     finder_test.go:149: sample_size=40 suggested=69, optimal=402, penalty(/100)=36.03
//     finder_test.go:149: sample_size=80 suggested=79, optimal=389, penalty(/100)=34.05
// === RUN   TestSplitFinderDistributions/span=50/length=uniform/access=zipf
//     finder_test.go:149: sample_size=10 suggested=110, optimal=528, penalty(/100)=39.33
//     finder_test.go:149: sample_size=20 suggested=132, optimal=532, penalty(/100)=37.06
//     finder_test.go:149: sample_size=40 suggested=209, optimal=528, penalty(/100)=28.94
// === RUN   TestSplitFinderDistributions/span=0/length=uniform/access=zipf
//     finder_test.go:149: sample_size=10 suggested=32, optimal=42, penalty(/100)=2.49
// === RUN   TestSplitFinderDistributions/span=95/length=uniform/access=uniform
//     finder_test.go:149: sample_size=10 suggested=50779, optimal=50535, penalty(/100)=0.27
func TestSplitFinderDistributions(t *testing.T) {
	const testingSeed = uint64(2400)
	const testingRecords = 25000

	makeSpan := func(startKey, length uint64) roachpb.Span {
		spanStartKey := uint32(startKey)
		spanEndKey := spanStartKey + uint32(length)
		return roachpb.Span{
			Key:    keys.SystemSQLCodec.TablePrefix(spanStartKey),
			EndKey: keys.SystemSQLCodec.TablePrefix(spanEndKey),
		}
	}

	intN := func(seed uint64) func(int) int {
		return rand.New(rand.NewSource(seed)).Intn
	}

	spanBool := func(seed uint64, percent float64) func() bool {
		gen := rand.New(rand.NewSource(seed))
		return func() bool {
			return gen.Float64() < percent
		}
	}

	uniformInt := func(seed, min, max uint64) func() uint64 {
		gen := rand.New(rand.NewSource(seed))
		return func() uint64 {
			return gen.Uint64n(max-min+1) + min
		}
	}

	zipfInt := func(seed, min, max uint64, theta float64) func() uint64 {
		gen := rand.NewZipf(rand.New(rand.NewSource(seed)), 1.1, 1, max-min)
		return func() uint64 {
			return gen.Uint64() + min
		}
	}

	testCases := []struct {
		desc string
		// randGenerator
		span         func() bool
		length       func() uint64
		access       func() uint64
		expectSplit  bool
		records, rng int
	}{
		{
			desc:        "span=95/length=uniform/access=zipf",
			span:        spanBool(testingSeed, 0.95),
			length:      uniformInt(testingSeed+1, 1, 1000),
			access:      zipfInt(testingSeed+2, 0, 100000, 0.99),
			expectSplit: true,
			rng:         110000,
		},
		{
			desc:        "span=50/length=zipf/access=zipf",
			span:        spanBool(testingSeed, 0.50),
			length:      zipfInt(testingSeed+2, 0, 1000, 0.99),
			access:      zipfInt(testingSeed+2, 0, 100000, 0.99),
			expectSplit: true,
			rng:         110000,
		},
		{
			desc:        "span=50/length=uniform/access=zipf",
			span:        spanBool(testingSeed, 0.50),
			length:      uniformInt(testingSeed+1, 1, 1000),
			access:      zipfInt(testingSeed+2, 0, 100000, 0.99),
			expectSplit: true,
			rng:         110000,
		},
		{
			desc:        "span=0/length=uniform/access=zipf",
			span:        spanBool(testingSeed, 0),
			length:      uniformInt(testingSeed+1, 1, 1000),
			access:      zipfInt(testingSeed+2, 0, 100000, 0.99),
			expectSplit: true,
			rng:         110000,
		},
		{
			desc:        "span=95/length=uniform/access=uniform",
			span:        spanBool(testingSeed, 0.95),
			length:      uniformInt(testingSeed+1, 1, 1000),
			access:      uniformInt(testingSeed+2, 0, 100000),
			expectSplit: true,
			rng:         110000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.records == 0 {
				tc.records = testingRecords
			}

			run := func(sampleSize int) float64 {
				r := newRecorder(tc.rng)
				splitKeySampleSize = sampleSize
				finder := NewFinder(timeutil.Now())
				intNFn := intN(testingSeed + 1)

				for i := 0; i < tc.records; i++ {
					startKey := tc.access()
					length := uint64(1)
					if tc.span() {
						length = tc.length()
					}

					r.record(startKey, startKey+length)
					finder.Record(makeSpan(startKey, length), intNFn)
				}
				splitKey := finder.Key()
				if splitKey == nil {
					optimal, _ := r.optimal(-1)
					t.Logf("sample_size=%d suggested=%d, optimal=%d, penalty(/100)=%0.2f", splitKeySampleSize, -1, optimal, 100.0)
					return 1
				}
				_, spk, _ := keys.SystemSQLCodec.DecodeTablePrefix(splitKey)
				suggestedSplitKey, _ := strconv.Atoi(fmt.Sprintf("%d", spk))
				optimal, penalty := r.optimal(suggestedSplitKey)
				penaltyRatio := (float64(penalty) / float64(r.count))
				t.Logf("sample_size=%d suggested=%d, optimal=%d, penalty(/100)=%0.2f", splitKeySampleSize, suggestedSplitKey, optimal, penaltyRatio*100)
				return penaltyRatio
			}

			for i := 10; run(i) > 0.35 && i < 10000; i *= 2 {
			}
		})
	}
}

type recorder struct {
	buckets               []int
	begin, step, n, count int
}

func newRecorder(rng int) *recorder {
	return &recorder{
		buckets: make([]int, rng+1),
		step:    1,
		n:       rng,
	}
}

func (r *recorder) record(start, end uint64) {
	if start < uint64(r.begin) {
		return
	}
	left := int((start - uint64(r.begin)) / uint64(r.step))
	right := int((end - uint64(r.begin)) / uint64(r.step))

	if left < 0 || right >= r.n || left > right {
		fmt.Printf("programming error [%d,%d] (l=%d,r=%d)\n", start, end, left, right)
		return
	}

	for i := left; i < right+1; i++ {
		r.buckets[i]++
		r.count++
	}
}

func (r *recorder) optimal(key int) (int, int) {
	sum := 0
	penalty := 0
	mid := 0
	found := false
	for i, v := range r.buckets {
		sum += v
		if 2*sum >= r.count && !found {
			found = true
			mid = i * r.step
		}
		if i >= key && !found {
			penalty += v
		}
		if i <= key && found {
			penalty += v
		}
	}
	return mid, penalty
}
