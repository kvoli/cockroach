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

import "github.com/cockroachdb/cockroach/pkg/util/syncutil"

type Callback func(val float64)

func RegisterCallback(cb Callback) (id int64) {
	globallyRegisteredCallbacks.mu.Lock()
	defer globallyRegisteredCallbacks.mu.Unlock()

	id = globallyRegisteredCallbacks.mu.nextID
	globallyRegisteredCallbacks.mu.nextID++
	globallyRegisteredCallbacks.mu.callbacks = append(globallyRegisteredCallbacks.mu.callbacks,
		callbackWithID{
			id: id,
			cb: cb,
		})
	return id
}

// UnregisterCallback unregisters the callback to be run with observed
// scheduling latencies.
func UnregisterCallback(id int64) {
	globallyRegisteredCallbacks.mu.Lock()
	defer globallyRegisteredCallbacks.mu.Unlock()

	newCBs := []callbackWithID(nil)
	for i := range globallyRegisteredCallbacks.mu.callbacks {
		if globallyRegisteredCallbacks.mu.callbacks[i].id == id {
			continue
		}
		newCBs = append(newCBs, globallyRegisteredCallbacks.mu.callbacks[i])
	}
	globallyRegisteredCallbacks.mu.callbacks = newCBs
}

type callbackWithID struct {
	cb Callback
	id int64 // used to uniquely identify a registered callback; used when unregistering
}

var globallyRegisteredCallbacks = struct {
	mu struct {
		syncutil.Mutex
		nextID    int64 // used to allocate IDs to registered callbacks
		callbacks []callbackWithID
	}
}{}
