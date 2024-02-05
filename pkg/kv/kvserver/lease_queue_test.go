// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestLeaseQueueLeasePreferencePurgatoryError tests that not finding a
// lease transfer target whilst violating lease preferences, will put the
// replica in the lease queue purgatory.
func TestLeaseQueueLeasePreferencePurgatoryError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	skip.UnderRace(t) // too slow under stressrace
	skip.UnderDeadlock(t)
	skip.UnderShort(t)

	const initialPreferredNode = 1
	const nextPreferredNode = 2
	const numRanges = 40
	const numNodes = 3

	var blockTransferTarget atomic.Bool

	blockTransferTargetFn := func() bool {
		block := blockTransferTarget.Load()
		return block
	}

	knobs := base.TestingKnobs{
		Store: &kvserver.StoreTestingKnobs{
			AllocatorKnobs: &allocator.TestingKnobs{
				BlockTransferTarget: blockTransferTargetFn,
			},
		},
	}

	serverArgs := make(map[int]base.TestServerArgs, numNodes)
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Knobs: knobs,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "rack", Value: fmt.Sprintf("%d", i+1)}},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.Conns[0]
	setLeasePreferences := func(node int) {
		_, err := db.Exec(fmt.Sprintf(`ALTER TABLE t CONFIGURE ZONE USING
      num_replicas=3, num_voters=3, voter_constraints='[]', lease_preferences='[[+rack=%d]]'`,
			node))
		require.NoError(t, err)
	}

	leaseCount := func(node int) int {
		var count int
		err := db.QueryRow(fmt.Sprintf(
			"SELECT count(*) FROM [SHOW RANGES FROM TABLE t WITH DETAILS] WHERE lease_holder = %d", node),
		).Scan(&count)
		require.NoError(t, err)
		return count
	}

	checkLeaseCount := func(node, expectedLeaseCount int) error {
		if count := leaseCount(node); count != expectedLeaseCount {
			return errors.Errorf("expected %d leases on node %d, found %d",
				expectedLeaseCount, node, count)
		}
		return nil
	}

	// Create a test table with numRanges-1 splits, to end up with numRanges
	// ranges. We will use the test table ranges to assert on the purgatory lease
	// preference behavior.
	_, err := db.Exec("CREATE TABLE t (i int);")
	require.NoError(t, err)
	_, err = db.Exec(
		fmt.Sprintf("INSERT INTO t(i) select generate_series(1,%d)", numRanges-1))
	require.NoError(t, err)
	_, err = db.Exec("ALTER TABLE t SPLIT AT SELECT i FROM t;")
	require.NoError(t, err)
	require.NoError(t, tc.WaitForFullReplication())

	// Set a preference on the initial node, then wait until all the leases for
	// the test table are on that node.
	setLeasePreferences(initialPreferredNode)
	testutils.SucceedsSoon(t, func() error {
		for serverIdx := 0; serverIdx < numNodes; serverIdx++ {
			require.NoError(t, tc.GetFirstStoreFromServer(t, serverIdx).
				ForceLeaseQueueProcess())
		}
		return checkLeaseCount(initialPreferredNode, numRanges)
	})

	// Block returning transfer targets from the allocator, then update the
	// preferred node. We expect that every range for the test table will end up
	// in purgatory on the initially preferred node.
	store := tc.GetFirstStoreFromServer(t, 0)
	blockTransferTarget.Store(true)
	setLeasePreferences(nextPreferredNode)
	testutils.SucceedsSoon(t, func() error {
		require.NoError(t, store.ForceLeaseQueueProcess())
		if purgLen := store.LeaseQueuePurgatoryLength(); purgLen != numRanges {
			return errors.Errorf("expected %d in purgatory but got %v", numRanges, purgLen)
		}
		return nil
	})

	// Lastly, unblock returning transfer targets. Expect that the leases from
	// the test table all move to the new preference. Note we don't force a
	// replication queue scan, as the purgatory retry should handle the
	// transfers.
	blockTransferTarget.Store(false)
	testutils.SucceedsSoon(t, func() error {
		return checkLeaseCount(nextPreferredNode, numRanges)
	})
}
