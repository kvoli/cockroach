// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocatorimpl
//
// import (
// 	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/state"
// 	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
// 	"github.com/cockroachdb/cockroach/pkg/roachpb"
// )

// func bestStoreToMinimizeDistance(
// 	impact state.DimensionContainer,
// 	existing roachpb.StoreID,
// 	candidates []roachpb.StoreID,
// 	storeDescMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
// 	options *LoadScorerOptions,
// ) (bestCandidate roachpb.StoreID, reason declineReason) {
// 	storeLoadMap := make(map[roachpb.StoreID]state.DimensionContainer, len(candidates)+1)
// 	for _, store := range candidates {
// 		if desc, ok := storeDescMap[store]; ok {
// 			storeLoadMap[store] = desc.Capacity.Dimensions()
// 		}
// 	}
// 	desc, ok := storeDescMap[existing]
// 	if !ok {
// 		return 0, missingStatsForExistingStore
// 	}
// 	storeLoadMap[existing] = desc.Capacity.Dimensions()
//
// 	// domain defines the domain over which this function tries to minimize the
// 	// load distance.
// 	domain := append(candidates, existing)
// 	storeDescs := make([]roachpb.StoreDescriptor, 0, len(domain))
// 	for _, desc := range storeDescMap {
// 		storeDescs = append(storeDescs, *desc)
// 	}
// 	domainStoreList := storepool.MakeStoreList(storeDescs)
//
// 	return bestCandidate, shouldRebalance
// }
//
// func candidateDistances(
// 	storeLoadMap map[roachpb.StoreID]state.DimensionContainer,
// 	candidates []roachpb.StoreID,
// ) {
// 	distances := make([][]float64, len(candidates)+1)
// 	for i := range distances {
//
//
// 	}
// }
//
//
// func distance(a, b state.LoadDimension) float64 {
// 	return 0
// }
