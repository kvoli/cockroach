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
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
)

func registerAllocator(r registry.Registry) {
	runAllocator := func(ctx context.Context, t test.Test, c cluster.Cluster, start int, maxStdDev float64) {
		c.Put(ctx, t.Cockroach(), "./cockroach")

		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.ExtraArgs = []string{"--vmodule=store_rebalancer=5,allocator=5,allocator_scorer=5,replicate_queue=5"}
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Range(1, start))
		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()

		m := c.NewMonitor(ctx, c.Range(1, start))
		m.Go(func(ctx context.Context) error {
			t.Status("loading fixture")
			if err := c.RunE(
				ctx, c.Node(1), "./cockroach", "workload", "fixtures", "import", "tpch", "--scale-factor", "1",
			); err != nil {
				t.Fatal(err)
			}
			return nil
		})
		m.Wait()

		// Start the remaining nodes to kick off upreplication/rebalancing.
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Range(start+1, c.Spec().NodeCount))

		c.Run(ctx, c.Node(1), `./cockroach workload init kv --drop`)
		t.Status("starting workload")
		for node := 1; node <= c.Spec().NodeCount; node++ {
			node := node
			// TODO(dan): Ideally, the test would fail if this queryload failed,
			// but we can't put it in monitor as-is because the test deadlocks.
			go func() {
				const cmd = `./cockroach workload run kv --tolerate-errors --min-block-bytes=8 --max-block-bytes=127`
				l, err := t.L().ChildLogger(fmt.Sprintf(`kv-%d`, node))
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				_ = c.RunE(ctx, c.Node(node), cmd)
			}()
		}

		m = c.NewMonitor(ctx, c.All())
		m.Go(func(ctx context.Context) error {
			t.Status("waiting for reblance")
			err := waitForRebalance(ctx, t.L(), db, maxStdDev)
			if err == nil {
				// Setup the roachperf reporting.
			}
			return err
		})

		// Tick once before starting the backup, and once after to capture the
		// total elapsed time.
		tick, perfBuf := initAllocatorJobPerfArtifacts(t.Name(), 2*time.Hour)
		tick()
		m.Wait()
		tick()

		dest := filepath.Join(t.PerfArtifactsDir(), "stats.json")
		if err := c.RunE(ctx, c.Node(1), "mkdir -p "+filepath.Dir(dest)); err != nil {
			log.Errorf(ctx, "failed to create perf dir: %+v", err)
		}
		if err := c.PutString(ctx, perfBuf.String(), dest, 0755, c.Node(1)); err != nil {
			log.Errorf(ctx, "failed to upload perf artifacts to node: %s", err.Error())
		}
	}

	r.Add(registry.TestSpec{
		Name:    `replicate/up/1to3`,
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(3),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runAllocator(ctx, t, c, 1, 10.0)
		},
	})
	r.Add(registry.TestSpec{
		Name:    `replicate/rebalance/3to5`,
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(5),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runAllocator(ctx, t, c, 3, 42.0)
		},
	})
	r.Add(registry.TestSpec{
		Name:    `replicate/wide`,
		Owner:   registry.OwnerKV,
		Timeout: 10 * time.Minute,
		Cluster: r.MakeClusterSpec(9, spec.CPU(1)),
		Run:     runWideReplication,
	})
}

type rebalanceStats struct {
	rebalanceInterval  int64
	rangeEventCount    int64
	replicaCountStdDev float64
	rangesPerStore     map[int64]int64
}

// initAllocatorJobPerfArtifacts a histogram, creates a performance
// artifact directory and returns a method that when invoked records a tick.
func initAllocatorJobPerfArtifacts(testName string, timeout time.Duration) (func(), *bytes.Buffer) {
	// Register a named histogram to track the total time the allocator took.
	// Roachperf uses this information to display information about this
	// roachtest.
	reg := histogram.NewRegistry(
		timeout,
		histogram.MockWorkloadName,
	)
	reg.GetHandle().Get(testName)

	bytesBuf := bytes.NewBuffer([]byte{})
	jsonEnc := json.NewEncoder(bytesBuf)
	tick := func() {
		reg.Tick(func(tick histogram.Tick) {
			_ = jsonEnc.Encode(tick.Snapshot())
		})
	}

	return tick, bytesBuf
}

func getRebalanceStats(db *gosql.DB) (*rebalanceStats, error) {
	rebalanceStats := rebalanceStats{rangesPerStore: make(map[int64]int64)}

	// Output time it took to rebalance.
	{
		var rebalanceIntervalStr string
		if err := db.QueryRow(
			`SELECT (SELECT max(timestamp) FROM system.rangelog) - `+
				`(SELECT max(timestamp) FROM system.eventlog WHERE "eventType"=$1)`,
			`node_join`, /* sql.EventLogNodeJoin */
		).Scan(&rebalanceIntervalStr); err != nil {
			return nil, err
		}
		timeInterval, err := time.Parse("15:04:05.000000", rebalanceIntervalStr)
		interval := ((timeInterval.Hour()*60)+timeInterval.Minute())*60 + timeInterval.Second()

		if err != nil {
			return nil, err
		}
		rebalanceStats.rebalanceInterval = int64(interval)
	}

	// Output # of range events that occurred. All other things being equal,
	// larger numbers are worse and potentially indicate thrashing.
	{
		var rangeEvents int64
		q := `SELECT count(*) from system.rangelog`
		if err := db.QueryRow(q).Scan(&rangeEvents); err != nil {
			return nil, err
		}
		rebalanceStats.rangeEventCount = rangeEvents
	}

	// Output standard deviation of the replica counts for all stores.
	{
		var stdDev float64
		if err := db.QueryRow(
			`SELECT stddev(range_count) FROM crdb_internal.kv_store_status`,
		).Scan(&stdDev); err != nil {
			return nil, err
		}
		rebalanceStats.replicaCountStdDev = stdDev
	}

	// Output the number of ranges on each store.
	{
		rows, err := db.Query(`SELECT store_id, range_count FROM crdb_internal.kv_store_status`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var storeID, rangeCount int64
			if err := rows.Scan(&storeID, &rangeCount); err != nil {
				return nil, err
			}
			rebalanceStats.rangesPerStore[storeID] = rangeCount
		}
	}
	return &rebalanceStats, nil
}

// printRebalanceStats prints the time it took for rebalancing to finish and the
// final standard deviation of replica counts across stores.
func printRebalanceStats(l *logger.Logger, stats rebalanceStats) {
	l.Printf("cluster took %f to rebalance\n", stats.rebalanceInterval)
	l.Printf("%d range events\n", stats.rangeEventCount)
	l.Printf("stdDev(replica count) = %.2f\n", stats.replicaCountStdDev)
	for storeID, rangeCount := range stats.rangesPerStore {
		l.Printf("s%d has %d ranges\n", storeID, rangeCount)
	}
}

type replicationStats struct {
	SecondsSinceLastEvent int64
	EventType             string
	RangeID               int64
	StoreID               int64
	ReplicaCountStdDev    float64
}

func (s replicationStats) String() string {
	return fmt.Sprintf("last range event: %s for range %d/store %d (%ds ago)",
		s.EventType, s.RangeID, s.StoreID, s.SecondsSinceLastEvent)
}

// allocatorStats returns the duration of stability (i.e. no replication
// changes) and the standard deviation in replica counts. Only unrecoverable
// errors are returned.
func allocatorStats(db *gosql.DB) (s replicationStats, err error) {
	defer func() {
		if err != nil {
			s.ReplicaCountStdDev = math.MaxFloat64
		}
	}()

	// NB: These are the storage.RangeLogEventType enum, but it's intentionally
	// not used to avoid pulling in the dep.
	eventTypes := []interface{}{
		// NB: these come from storagepb.RangeLogEventType.
		`split`, `add_voter`, `remove_voter`,
	}

	q := `SELECT extract_duration(seconds FROM now()-timestamp), "rangeID", "storeID", "eventType"` +
		`FROM system.rangelog WHERE "eventType" IN ($1, $2, $3) ORDER BY timestamp DESC LIMIT 1`

	row := db.QueryRow(q, eventTypes...)
	if row == nil {
		// This should never happen, because the archived store we're starting with
		// will always have some range events.
		return replicationStats{}, errors.New("couldn't find any range events")
	}
	if err := row.Scan(&s.SecondsSinceLastEvent, &s.RangeID, &s.StoreID, &s.EventType); err != nil {
		return replicationStats{}, err
	}

	if err := db.QueryRow(
		`SELECT stddev(range_count) FROM crdb_internal.kv_store_status`,
	).Scan(&s.ReplicaCountStdDev); err != nil {
		return replicationStats{}, err
	}

	return s, nil
}

// waitForRebalance waits until there's been no recent range adds, removes, and
// splits. We wait until the cluster is balanced or until `StableInterval`
// elapses, whichever comes first. Then, it prints stats about the rebalancing
// process. If the replica count appears unbalanced, an error is returned.
//
// This method is crude but necessary. If we were to wait until range counts
// were just about even, we'd miss potential post-rebalance thrashing.
func waitForRebalance(
	ctx context.Context, l *logger.Logger, db *gosql.DB, maxStdDev float64,
) error {
	// const statsInterval = 20 * time.Second
	const statsInterval = 2 * time.Second
	const stableSeconds = 3 * 60

	var statsTimer timeutil.Timer
	defer statsTimer.Stop()
	statsTimer.Reset(statsInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-statsTimer.C:
			statsTimer.Read = true
			stats, err := allocatorStats(db)
			if err != nil {
				return err
			}

			l.Printf("%v\n", stats)
			if stableSeconds <= stats.SecondsSinceLastEvent {
				l.Printf("replica count stddev = %f, max allowed stddev = %f\n", stats.ReplicaCountStdDev, maxStdDev)
				if stats.ReplicaCountStdDev > maxStdDev {
					rebalanceStats, err := getRebalanceStats(db)
					if err != nil {
						printRebalanceStats(l, *rebalanceStats)
					}
					return errors.Errorf(
						"%ds elapsed without changes, but replica count standard "+
							"deviation is %.2f (>%.2f)", stats.SecondsSinceLastEvent,
						stats.ReplicaCountStdDev, maxStdDev)
				}
				return nil
			}
			statsTimer.Reset(statsInterval)
		}
	}

}

func runWideReplication(ctx context.Context, t test.Test, c cluster.Cluster) {
	nodes := c.Spec().NodeCount
	if nodes != 9 {
		t.Fatalf("9-node cluster required")
	}

	c.Put(ctx, t.Cockroach(), "./cockroach")
	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.ExtraArgs = []string{"--vmodule=replicate_queue=6"}
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
	c.Start(ctx, t.L(), startOpts, settings, c.All())

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	zones := func() []string {
		rows, err := db.Query(`SELECT target FROM crdb_internal.zones`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var results []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			results = append(results, name)
		}
		return results
	}

	run := func(stmt string) {
		t.L().Printf("%s\n", stmt)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	setReplication := func(width int) {
		// Change every zone to have the same number of replicas as the number of
		// nodes in the cluster.
		for _, zone := range zones() {
			run(fmt.Sprintf(`ALTER %s CONFIGURE ZONE USING num_replicas = %d`, zone, width))
		}
	}
	setReplication(nodes)

	countMisreplicated := func(width int) int {
		var count int
		if err := db.QueryRow(
			"SELECT count(*) FROM crdb_internal.ranges WHERE array_length(replicas,1) != $1",
			width,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	waitForReplication := func(width int) {
		for count := -1; count != 0; time.Sleep(time.Second) {
			count = countMisreplicated(width)
			t.L().Printf("%d mis-replicated ranges\n", count)
		}
	}

	waitForReplication(nodes)

	numRanges := func() int {
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM crdb_internal.ranges`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}()

	// Stop the cluster and restart 2/3 of the nodes.
	c.Stop(ctx, t.L(), option.DefaultStopOpts())
	tBeginDown := timeutil.Now()
	c.Start(ctx, t.L(), startOpts, settings, c.Range(1, 6))

	waitForUnderReplicated := func(count int) {
		for start := timeutil.Now(); ; time.Sleep(time.Second) {
			query := `
SELECT sum((metrics->>'ranges.unavailable')::DECIMAL)::INT AS ranges_unavailable,
       sum((metrics->>'ranges.underreplicated')::DECIMAL)::INT AS ranges_underreplicated
FROM crdb_internal.kv_store_status
`
			var unavailable, underReplicated int
			if err := db.QueryRow(query).Scan(&unavailable, &underReplicated); err != nil {
				t.Fatal(err)
			}
			t.L().Printf("%d unavailable, %d under-replicated ranges\n", unavailable, underReplicated)
			if unavailable != 0 {
				// A freshly started cluster might show unavailable ranges for a brief
				// period of time due to the way that metric is calculated. Only
				// complain about unavailable ranges if they persist for too long.
				if timeutil.Since(start) >= 30*time.Second {
					t.Fatalf("%d unavailable ranges", unavailable)
				}
				continue
			}
			if underReplicated >= count {
				break
			}
		}
	}

	waitForUnderReplicated(numRanges)
	if n := countMisreplicated(9); n != 0 {
		t.Fatalf("expected 0 mis-replicated ranges, but found %d", n)
	}

	decom := func(id int) {
		c.Run(ctx, c.Node(1),
			fmt.Sprintf("./cockroach node decommission --insecure --wait=none %d", id))
	}

	// Decommission a node. The ranges should down-replicate to 7 replicas.
	decom(9)
	waitForReplication(7)

	// Set the replication width to 5. The replicas should down-replicate, though
	// this currently requires the time-until-store-dead threshold to pass
	// because the allocator cannot select a replica for removal that is on a
	// store for which it doesn't have a store descriptor.
	run(`SET CLUSTER SETTING server.time_until_store_dead = '90s'`)
	// Sleep until the node is dead so that when we actually wait for replication,
	// we can expect things to move swiftly.
	time.Sleep(90*time.Second - timeutil.Since(tBeginDown))

	setReplication(5)
	waitForReplication(5)

	// Restart the down nodes to prevent the dead node detector from complaining.
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(7, 9))
}
