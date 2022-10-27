package state

import (
	"math"
	"math/bits"

	"github.com/cockroachdb/errors"
)

type bitSet uint64

const maxSetSize = 63

// add returns a copy of the bitSet with the given element added.
func (s bitSet) add(idx uint64) bitSet {
	if idx > maxSetSize {
		panic(errors.AssertionFailedf("cannot insert %d into bitSet", idx))
	}
	return s | (1 << idx)
}

// union returns the set union of this set with the given set.
func (s bitSet) union(o bitSet) bitSet {
	return s | o
}

// intersection returns the set intersection of this set with the given set.
func (s bitSet) intersection(o bitSet) bitSet {
	return s & o
}

// difference returns the set difference of this set with the given set.
func (s bitSet) difference(o bitSet) bitSet {
	return s & ^o
}

// intersects returns true if this set and the given set intersect.
func (s bitSet) intersects(o bitSet) bool {
	return s.intersection(o) != 0
}

// isSubsetOf returns true if this set is a subset of the given set.
func (s bitSet) isSubsetOf(o bitSet) bool {
	return s.union(o) == o
}

// isSingleton returns true if the set has exactly one element.
func (s bitSet) isSingleton() bool {
	return s > 0 && (s&(s-1)) == 0
}

// next returns the next element in the set after the given start index, and
// a bool indicating whether such an element exists.
func (s bitSet) next(startVal uint64) (elem uint64, ok bool) {
	if startVal < maxSetSize {
		if ntz := bits.TrailingZeros64(uint64(s >> startVal)); ntz < 64 {
			return startVal + uint64(ntz), true
		}
	}
	return uint64(math.MaxInt64), false
}

// len returns the number of elements in the set.
func (s bitSet) len() int {
	return bits.OnesCount64(uint64(s))
}
