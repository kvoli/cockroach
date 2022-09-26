// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func registerRebalanceLoad(r registry.Registry) {
	// This test creates a single table for kv to use and splits the table to
	// have 100 ranges for every node in the cluster. The test then asserts
	// that replica, lease and QPS count for every store converges to within
	// the range [underfullThreshold, overfullThreshold] for each load signal.
	// These thresholds are taken from the defaults directly.
	rebalanceLoadRun := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
		rebalanceMode string,
		maxDuration time.Duration,
		concurrency int,
		mixedVersion bool,
	) {
		startOpts := option.DefaultStartOpts()
		roachNodes := c.Range(1, c.Spec().NodeCount-1)
		appNode := c.Node(c.Spec().NodeCount)
		numStores := len(roachNodes)
		if c.Spec().SSDs > 1 && !c.Spec().RAID0 {
			numStores *= c.Spec().SSDs
			startOpts.RoachprodOpts.StoreCount = c.Spec().SSDs
		}
		// There will be around 45 system ranges. Add an additional 100 per
		// store, so that the there are enough to assert on replica/range
		// count balance.
		splits := numStores * 100
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
			"--vmodule=store_rebalancer=5,allocator=5,allocator_scorer=5,replicate_queue=5")
		settings := install.MakeClusterSettings()
		if mixedVersion {
			predecessorVersion, err := PredecessorVersion(*t.BuildVersion())
			require.NoError(t, err)
			settings.Binary = uploadVersion(ctx, t, c, c.All(), predecessorVersion)
			// Upgrade some (or all) of the first N-1 CRDB nodes. We ignore the last
			// CRDB node (to leave at least one node on the older version), and the
			// app node.
			lastNodeToUpgrade := rand.Intn(c.Spec().NodeCount-2) + 1
			t.L().Printf("upgrading %d nodes to the current cockroach binary", lastNodeToUpgrade)
			nodesToUpgrade := c.Range(1, lastNodeToUpgrade)
			// An empty string means that the cockroach binary specified by the
			// `cockroach` flag will be used.
			const newVersion = ""
			c.Start(ctx, t.L(), startOpts, settings, roachNodes)
			upgradeNodes(ctx, nodesToUpgrade, startOpts, newVersion, t, c)
		} else {
			c.Put(ctx, t.Cockroach(), "./cockroach", roachNodes)
			c.Start(ctx, t.L(), startOpts, settings, roachNodes)
		}

		c.Put(ctx, t.DeprecatedWorkload(), "./workload", appNode)
		c.Run(ctx, appNode, fmt.Sprintf("./workload init kv --drop --splits=%d {pgurl:1}", splits))

		var m *errgroup.Group // see comment in version.go
		m, ctx = errgroup.WithContext(ctx)

		// Setup the prometheus instance and client.
		promNode := c.Node(c.Spec().NodeCount)
		cfg := (&prometheus.Config{}).
			WithCluster(roachNodes.InstallNodes()).
			WithPrometheusNode(promNode.InstallNodes()[0])

		err := c.StartGrafana(ctx, t.L(), cfg)
		require.NoError(t, err)

		cleanupFunc := func() {
			if err := c.StopGrafana(ctx, t.L(), t.ArtifactsDir()); err != nil {
				t.L().ErrorfCtx(ctx, "Error(s) shutting down prom/grafana %s", err)
			}
		}
		defer cleanupFunc()

		promClient, err := clusterstats.SetupCollectorPromClient(ctx, c, t.L(), cfg)
		require.NoError(t, err)

		// Setup the stats collector for the prometheus client.
		statCollector := clusterstats.NewStatsCollector(ctx, promClient)

		// Enable us to exit out of workload early when we achieve the desired
		// load balance. This drastically shortens the duration of the test in the
		// common case.
		ctx, cancel := context.WithCancel(ctx)

		m.Go(func() error {
			t.L().Printf("starting load generator\n")

			err := c.RunE(ctx, appNode, fmt.Sprintf(
				"./workload run kv --read-percent=95 --tolerate-errors --concurrency=%d "+
					"--duration=%v {pgurl:1-%d}",
				concurrency, maxDuration, len(roachNodes)))
			if errors.Is(ctx.Err(), context.Canceled) {
				// We got canceled either because lease balance was achieved or the
				// other worker hit an error. In either case, it's not this worker's
				// fault.
				return nil
			}
			return err
		})

		m.Go(func() error {
			t.Status("checking for load balance")

			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()

			t.Status("disable load based splitting")
			if err := disableLoadBasedSplitting(ctx, db); err != nil {
				return err
			}

			if _, err := db.ExecContext(
				ctx, `SET CLUSTER SETTING kv.allocator.load_based_rebalancing=$1::string`, rebalanceMode,
			); err != nil {
				return err
			}

			require.NoError(t, WaitFor3XReplication(ctx, t, db))
			for tBegin := timeutil.Now(); timeutil.Since(tBegin) <= maxDuration; {

				if done, err := isLoadEvenlyDistributed(ctx, t.L(), statCollector); err != nil {
					return err
				} else if done {
					t.Status("successfully balanced; waiting for kv to finish running")
					cancel()
					return nil
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
				}
			}

			return fmt.Errorf("timed out before load was evenly spread")
		})
		if err := m.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	concurrency := 128

	r.Add(
		registry.TestSpec{
			Name:    `rebalance/by-load/leases`,
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(4), // the last node is just used to generate load
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					concurrency = 32
					fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
				}
				rebalanceLoadRun(ctx, t, c, "leases", 3*time.Minute, concurrency, false /* mixedVersion */)
			},
		},
	)
	r.Add(
		registry.TestSpec{
			Name:    `rebalance/by-load/leases/mixed-version`,
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(4), // the last node is just used to generate load
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					concurrency = 32
					fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
				}
				rebalanceLoadRun(ctx, t, c, "leases", 3*time.Minute, concurrency, true /* mixedVersion */)
			},
		},
	)
	r.Add(
		registry.TestSpec{
			Name:    `rebalance/by-load/replicas`,
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(7), // the last node is just used to generate load
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					concurrency = 32
					fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
				}
				rebalanceLoadRun(
					ctx, t, c, "leases and replicas", 5*time.Minute, concurrency, false, /* mixedVersion */
				)
			},
		},
	)
	r.Add(
		registry.TestSpec{
			Name:    `rebalance/by-load/replicas/mixed-version`,
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(7), // the last node is just used to generate load
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					concurrency = 32
					fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
				}
				rebalanceLoadRun(
					ctx, t, c, "leases and replicas", 5*time.Minute, concurrency, true, /* mixedVersion */
				)
			},
		},
	)
	cSpec := r.MakeClusterSpec(7, spec.SSD(2)) // the last node is just used to generate load
	var skip string
	if cSpec.Cloud != spec.GCE {
		skip = fmt.Sprintf("multi-store tests are not supported on cloud %s", cSpec.Cloud)
	}
	r.Add(
		registry.TestSpec{
			Skip:    skip,
			Name:    `rebalance/by-load/replicas/ssds=2`,
			Owner:   registry.OwnerKV,
			Cluster: cSpec,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					t.Fatal("cannot run multi-store in local mode")
				}
				rebalanceLoadRun(
					ctx, t, c, "leases and replicas", 10*time.Minute, concurrency, false, /* mixedVersion */
				)
			},
		},
	)
}

func collectStoreStat(
	ctx context.Context, l *logger.Logger, statCollector clusterstats.StatCollector, query string,
) (map[int]int, error) {
	now := timeutil.Now()

	stats, err := statCollector.CollectPoint(ctx, l, now, query)
	if err != nil {
		return nil, err
	}

	counts := make(map[int]int)
	for store, val := range stats["store"] {
		storeID, err := strconv.Atoi(store)
		if err != nil {
			return nil, err
		}
		counts[storeID] = int(val.Value)
	}
	return counts, nil
}

func isLoadEvenlyDistributed(
	ctx context.Context, l *logger.Logger, statCollector clusterstats.StatCollector,
) (bool, error) {
	leaseCounts, err := collectStoreStat(ctx, l, statCollector, "replicas_leaseholders")
	if err != nil {
		return false, err
	}

	qpsCounts, err := collectStoreStat(ctx, l, statCollector, "rebalancing_queriespersecond")
	if err != nil {
		return false, err
	}

	replicaCounts, err := collectStoreStat(ctx, l, statCollector, "replicas")
	if err != nil {
		return false, err
	}

	leaseCountsBalanced := checkLoadDistribution(l, leaseCounts, allocatorimpl.LeaseRebalanceThreshold, "leases")
	replicaCountsBalanced := checkLoadDistribution(l, replicaCounts, allocatorimpl.RangeRebalanceThreshold.Default(), "replicas")
	qpsCountsBalanced := checkLoadDistribution(l, qpsCounts, allocator.QPSRebalanceThreshold.Default(), "QPS")

	if leaseCountsBalanced && replicaCountsBalanced && qpsCountsBalanced {
		l.Printf("Leases, replicas and QPS are balanced.")
		return true, nil
	}
	return false, nil
}

func checkLoadDistribution(
	l *logger.Logger, distribution map[int]int, threshold float64, loadType string,
) bool {
	sum := 0
	count := len(distribution)
	for _, load := range distribution {
		sum += load
	}
	avg := float64(sum) / float64(count)
	overfullThreshold := avg * (1 + threshold)
	underfullThreshold := avg * (1 - threshold)

	for store, load := range distribution {
		if float64(load) > overfullThreshold {
			l.Printf("%s is not balanced, store %d is overfull: %v\n", loadType, store, formatStoreCounts(distribution))
			return false
		}
		if float64(load) < underfullThreshold {
			l.Printf("%s is not balanced, store %d is underfull: %v\n", loadType, store, formatStoreCounts(distribution))
			return false
		}
	}

	l.Printf("%s is balanced: %s\n", loadType, formatStoreCounts(distribution))
	return true
}

func formatStoreCounts(counts map[int]int) string {
	storeIDs := make([]int, 0, len(counts))
	for storeID := range counts {
		storeIDs = append(storeIDs, storeID)
	}
	sort.Ints(storeIDs)
	strs := make([]string, 0, len(counts))
	for _, storeID := range storeIDs {
		strs = append(strs, fmt.Sprintf("s%d: %d", storeID, counts[storeID]))
	}
	return fmt.Sprintf("[%s]", strings.Join(strs, ", "))
}
