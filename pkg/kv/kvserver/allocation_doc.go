package kvserver


/*
Package allocation deals with the assignment of replicas and the lease for a
range onto stores. The assignment is dynamic, both with regards to the number
of replicas and stores as well reassignment that may take place to more
efficienty service requests.



type StorePlanner interface {
	Plan(Store) Plan
}

// StoreRebalancePlanner has two methods which each return a plan to rebalance
// either ranges (and leases) or just leases.
type StoreRebalancePlanner interface {
	RebalanceRanges(Store) Plan
	RebalanceLeases(Store) Plan
}

// RangePlanner plans a change for a range, if planning is a good idea.
type RangePlanner interface {
	Plan(Range) Plan
}

// Allocator returns individual changes that should occur given a specific
// action and a range.
type Allocator struct {
	RebalanceTarget(Range)
	AllocateTarget(Range)
	RemoveTarget(Range)
	TransferTarget(Range)
}

func RebalanceTarget(Range) {
	candidateList := rankedCandidateListForRebalancing(Range)
	for (candidate: candidates) {
		target = bestRebalanceTarget(a.randGen, results)
		if simulateRemoveTarget() != target; break
	}
	select = power of 2(target)
	return
}

TransferTarget(Range):
bestStoreToMinimizeDelta()

need to replace QPS in:

- bestStoreToMinimizeDelta
- scorerOptions
	- minRequiredDiff (used in bestStoreToMinimizeDelta)
    - perReplica (used in bestStoreToMinimizeDelta)
	- thresholds

misc
- notion of cost/benefit for an action
- symmetric options between the replicate queue and store rebalancer
	- notion of cost is necessary so that only beneficial actions are taken
- store rebalancer logic
	- optimized for own store
    - optimized for getting rid of own replica
    - optimized for transferring lease


bestStoreToMinimizeDelta()
  this fn satisfies finding the candidate (c in C) to replace the existing (e)
  such that distance(e,min(c)) is minimized. Given the action represents d. 
    d = action
    o = minimum required diff

  - e > min(c) // in some dimension
  - e > mean(c) // in some dimension
  - e - d - min(c) > o
  - minimize distance(e,min(c))
  - 

  - generalization to k dimensions
  - minmax(distance(ci,cj)) in C
  - where only one action may be taken (o).

  - C must have an ordering (<)
  - ci,cj in C must have a distance
  - distance(ci, cj)

def cost_filter(existing: D, candidate: D, impact: D, minimum: D):
	not (existing - impact - candidate) <= minimum

def normalize_min_max(candidates: []D):
	max_dimensions = max(candidates)
	min_dimensions = min(candidates)
	map(candidates, c -> c / (max - min))
	return candidates

def normalize_over_under(candidates: []D):
	map(candidates, c -> c > over_threshold ? 1 :  c < under_threshold ? -1 : 0)

def minmax_dist(existing: D, candidates []D, impact: D):
	init_max_distance = candidate_distances(candidates)
	choices = []
	for (c : candidates):
		distance = max_distance(apply_impact(existing, c, impact))
		choices += (distance, candidate)

	sort(choices)
	filter(choices, less than init_max_distance)
	filter(choices, cost_filter)
	return choices

def apply_impact(existing: D, candidate D, impact D):
	return (existing - impact, candidate + impact)

def max_distance(candidates: []D):
	return max(candidate_distances(candidates))
	
def candidate_distances(candidates: []D) 
	distances = [][]
	for (i : candidates):
		for (j : candidates)
			distances[i][j] = distance(candidate[i], candidate[j])
	return distances

def distance(a: D, b: D):
	...
	chebyshev() // maximum distance dimension a,b
	canberra()  // sum over every dimension (a-b)/(a+b)
    eul()       // (a-b^p + ... + a-b^p)^1/p, p=2

*/
