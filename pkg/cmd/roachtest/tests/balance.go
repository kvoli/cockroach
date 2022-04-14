package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/prometheus"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

var (
	// TODO(kvoli): Need to decouple summary over a statistic here. One option
	// is to define some transformer funcs e.g. sum(), that apply to a query
	// and aggregate over them.
	qpsStat                 = clusterStat{tag: "qps", query: "rebalancing_queriespersecond"}
	rqpsStat                = clusterStat{tag: "rqps", query: "rebalancing_requestspersecond"}
	wpsStat                 = clusterStat{tag: "wps", query: "rebalancing_writespersecond"}
	rpsStat                 = clusterStat{tag: "rps", query: "rebalancing_readspersecond"}
	cpuStat                 = clusterStat{tag: "cpu", query: "sys_cpu_combined_percent-normalized"}
	ioReadStat              = clusterStat{tag: "io_read", query: "sys_host_disk_read_bytes"}
	ioWriteStat             = clusterStat{tag: "io_write", query: "sys_host_disk_write_bytes"}
	netSendStat             = clusterStat{tag: "net_send", query: "sys_host_net_send_bytes"}
	netRecvStat             = clusterStat{tag: "net_recv", query: "sys_host_net_recv_bytes"}
	rangeCountStat          = clusterStat{tag: "range_count", query: "ranges"}
	underreplicatedStat     = clusterStat{tag: "underreplicated", query: "ranges_underreplicated"}
	replicasStat            = clusterStat{tag: "replica_count", query: "replicas"}
	leaseTransferStat       = clusterStat{tag: "lease_transfers", query: "rebalancing_lease_transfers"}
	rangeRebalancesStat     = clusterStat{tag: "range_rebalance", query: "rebalancing_range_rebalances"}
	l0SublevelStat          = clusterStat{tag: "l0_sublevels", query: "storage_l0-sublevels"}
	l0FilesStat             = clusterStat{tag: "l0_files", query: "storage_l0-num-files"}
	admissionWaitStat       = clusterStat{tag: "admission_wait_time", query: "admission_wait_sum_kv"}
	admissionQLengthStat    = clusterStat{tag: "admission_q_length", query: "admission_wait_queue_length_kv"}
	admissionTotalSlotsStat = clusterStat{tag: "admission_total_slots", query: "admission_granter_total_slots_kv"}
	admissionUsedSlotsStat  = clusterStat{tag: "admission_used_slots", query: "admission_granter_used_slots_kv"}
)

func registerBalance(r registry.Registry) {
	runBalance := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		c.Put(ctx, t.Cockroach(), "./cockroach")

		startOpts := option.DefaultStartOpts()
		promNode := c.Spec().NodeCount - 1
		startNode := 1
		clusterNodes := c.Range(startNode, promNode)

		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), clusterNodes)

		statsCollector, err := newClusterStatsCollector(ctx, t, c, prometheus.DefaultScrapeInterval)
		if err != nil {
			return
		}
		defer statsCollector.cleanup()

		for node := 1; node < promNode; node++ {
			c.Run(ctx, c.Node(node), `./cockroach workload init kv --drop --splits 50`)
			t.Status("starting workload")
			node := node
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

		startTime := time.Now()

		iters := 0
		tickFn := func(stats []statEvent) bool {
			for _, stat := range stats {
				t.L().Printf("%s:%+v", stat.tag, stat.fromVal)
			}
			iters++
			return iters > 5
		}

		cs := newClusterStatStreamer(statsCollector, tickFn)
		cs.registerStat(qpsStat, rqpsStat, wpsStat, rpsStat, cpuStat)

		m := c.NewMonitor(ctx, clusterNodes)
		m.Go(func(ctx context.Context) error {
			t.Status("collecting stats")
			cs.runStatStreamer(ctx, t.L())
			endTime := time.Now()
			statsCollector.finalize(ctx,
				c,
				t,
				startTime,
				endTime,
				[]statSummaryQuery{
					{stat: qpsStat, aggQuery: query("sum(rebalancing_queriespersecond)")},
					{stat: wpsStat, aggQuery: query("sum(rebalancing_writespersecond)")},
				},
				func(summaries []StatSummary) float64 { return 1 },
			)
			return nil
		})

		m.Wait()
	}

	r.Add(registry.TestSpec{
		Name:    `balance/set`,
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runBalance(ctx, t, c)
		},
	})
}
