package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

// assertion thinking
// The idea here being that we should be able to assert on the throughput not
// dropping beyond a certain % of the throughput prior to limiting a node's
// capacity.
//
// Run the workload for 15 minutes to get baseline throughput
// Run the capacity limiting command
//
// Measure the workload throughput for the next 15 minutes
//   (a) expect throughput to not drop by more than X%
//   (b) measure the throughput at set marks (10s, 30s, 1m, 5m) and
//   assert/export the throughput then?
//
// Using the same workload each time
// kv --max-rate=5000 --min-block-bytes=2048 --max-block-bytes=2048 --concurrency=400
//

// TODO(kvoli):
// - make the test more general? and allow exporting timeseries to roachperf?
// detecting struggling replicas
//   - see replication AC doc, these would be enqueued into the replicate
//     queue.
//
// detecting struggling leaseholders
//   - this is trickier, we want to be deliberate about shedding the
//     leaseholders.
func registerLimitCapacity(r registry.Registry) {
	spec := func(subtest string, cfg limitCapacityOpts) registry.TestSpec {
		return registry.TestSpec{
			Name:             "limit_capacity/" + subtest,
			Owner:            registry.OwnerKV,
			Timeout:          1 * time.Hour,
			CompatibleClouds: registry.OnlyGCE,
			Suites:           registry.Suites(registry.Weekly),
			Cluster:          r.MakeClusterSpec(5, spec.CPU(8)),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runLimitCapacity(ctx, t, c, cfg)
			},
		}
	}
	r.Add(spec("write_b=50mb", newAllocatorOverloadOpts().limitWriteCap(52428800)))
	r.Add(spec("write_b=50mb/follower", newAllocatorOverloadOpts().limitWriteCap(52428800).followerOnly()))
	r.Add(spec("compact=0", newAllocatorOverloadOpts().limitCompaction(0)))
	r.Add(spec("compact=0/follower", newAllocatorOverloadOpts().limitCompaction(0).followerOnly()))
}

type limitCapacityOpts struct {
	writeCapBytes         int
	compactConcurrency    int
	noLeasesLimitedNode   bool
	requiredRelThroughput float64
}

func newAllocatorOverloadOpts() limitCapacityOpts {
	return limitCapacityOpts{
		writeCapBytes:      -1,
		compactConcurrency: -1,
	}
}

func (a limitCapacityOpts) limitCompaction(concurrency int) limitCapacityOpts {
	a.compactConcurrency = concurrency
	return a
}

func (a limitCapacityOpts) limitWriteCap(bytes int) limitCapacityOpts {
	a.writeCapBytes = bytes
	return a
}

func (a limitCapacityOpts) followerOnly() limitCapacityOpts {
	a.noLeasesLimitedNode = true
	return a
}

// runLimitCapacity sets up a kv50 fixed rate workload running against the
// first n-1 nodes, the last node is used for the workload. The n-1'th node has
// any defined capacity limitations applied to it. The test initially runs for
// 10 minutes to establish a baseline level of workload throughput, the
// capacity constraint is applied and after another 10 minutes the throughput
// is measured relative to the baseline QPS.
func runLimitCapacity(ctx context.Context, t test.Test, c cluster.Cluster, cfg limitCapacityOpts) {
	require.False(t, c.IsLocal())

	appNodeID := c.Spec().NodeCount
	limitedNodeID := c.Spec().NodeCount - 1
	nodes := c.Range(1, limitedNodeID)
	appNode := c.Node(appNodeID)
	limitedNode := c.Node(limitedNodeID)

	duration := 10 * time.Minute

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), nodes)
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	require.NoError(t, WaitFor3XReplication(ctx, t, conn))
	var cancels []func()

	c.Run(ctx, appNode, "./cockroach workload init kv --splits=1000 {pgurl:1}")

	if cfg.noLeasesLimitedNode {
		t.Status("configuring lease preferences to have no leases on n",
			limitedNodeID)
		configureAllZones(t, ctx, conn, zoneConfig{
			replicas:        3,
			leasePreference: fmt.Sprintf("[-node%d]", limitedNodeID),
		})
	}

	m := c.NewMonitor(ctx, nodes)
	cancels = append(cancels, m.GoWithCancel(func(ctx context.Context) error {
		t.L().Printf("starting load generator\n")
		// NB: kv50 with 4kb block size at 5k rate will incur approx. 500mb/s write
		// bandwidth after 10 minutes across the cluster. Spread across 4 CRDB
		// nodes, expect approx. 125 mb/s write bandwidth each and 30-50% CPU
		// utilization.
		err := c.RunE(ctx, appNode, fmt.Sprintf(
			"./cockroach workload run kv --read-percent=50 --tolerate-errors --concurrency=400 "+
				"--min-block-bytes=4096 --max-block-bytes=4096 --max-rate=5000 "+
				"--duration=30m {pgurl:1-%d}", c.Spec().NodeCount-2))
		return err
	}))

	t.Status(fmt.Sprintf("waiting %s for baseline workload throughput", duration))
	wait(c.NewMonitor(ctx, nodes), duration)
	qpsInitial := measureQPS(ctx, t, 10*time.Second, conn)
	t.Status(fmt.Sprintf("initial (single node) qps: %.0f", qpsInitial))

	if cfg.writeCapBytes >= 0 {
		c.Run(ctx, limitedNode, "sudo", "systemctl", "set-property", "cockroach-system",
			fmt.Sprintf("'IOWriteBandwidthMax=/mnt/data1 %d'", cfg.writeCapBytes))
	}

	if cfg.compactConcurrency >= 0 {
		cancels = append(cancels, m.GoWithCancel(func(ctx context.Context) error {
			t.Status(fmt.Sprintf("setting compaction concurrency on n%d to %d", limitedNodeID, cfg.compactConcurrency))
			limitedConn := c.Conn(ctx, t.L(), limitedNodeID)
			defer limitedConn.Close()

			// Execution will be blocked on setting the compaction concurrency. The
			// execution is cancelled below after measuring the final QPS below.
			_, err := limitedConn.ExecContext(ctx,
				fmt.Sprintf(`SELECT crdb_internal.set_compaction_concurrency(%d,%d,%d)`,
					limitedNodeID, limitedNodeID, cfg.compactConcurrency,
				))
			return err
		}))
	}

	wait(c.NewMonitor(ctx, nodes), duration)
	qpsFinal := measureQPS(ctx, t, 10*time.Second, conn)
	qpsRelative := qpsFinal / qpsInitial
	t.Status(fmt.Sprintf("initial qps=%f final qps=%f (%f%%)", qpsInitial, qpsFinal, 100*qpsRelative))
	for _, cancel := range cancels {
		cancel()
	}
}
