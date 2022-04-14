package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/prometheus"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// roachtest/roachperf harness for asserting and collecting cluster stats.
//
// goal: (1) visibility into allocator performance over time
//       (2) cheap testing and iteration on alloc changes
//       (3) insight into covar of cluster stats
//
// (1) streaming stats at an interval
//       - chan sending back stats (e.g. per store qps, measure of balance)
//       - can assert on these to move through testing settings  (when coefvar < 0.5, move hotkey)
//       - not persisted (transient, caller may maintain state for comparing ticks)
//       - not exported to roachperf, for the test to do interesting things with.
//
// (2) end of test snapshot summary
//       - single summary with all stats included
//       - fits into roachperf format
//         (i)  top level (scalar):
//             duration or avg time to rebalance or
//             steady state balance or
//             elapsed time w/ queuing or
//             elapsed time w/ stat exceeding threshold
//         (ii)  2nd level (timeseries):
//             aggregation over each stat (cluster view)
//             e.g. balance of qps, number of rebalancing ops
//                  lease transfers, underreplicated ranges
//         (iii) 3rd level (per ii, timeseries):
//             per store attribution to (ii)
//
//
// type perfRunOp struct {
// 	// ticks
// 	time []int64
// 	// e.g. stdev over time for qps
// 	summary []float64
// 	// e.g.  store 1..n qps over time
// 	detail map[string]*[]float64
// }
//
// type perfRun struct {
// 	// e.g test duration
// 	result float64
// 	// e.g. qps, cpu...
// 	perfRunDetail map[string]*perfRunOp
// }
//
type query string
type tag string

type clusterStat struct {
	query query
	tag   tag
}

type clusterStatCollector struct {
	promClient promv1.API
	interval   time.Duration
	cleanup    func()
}

func newClusterStatsCollector(
	ctx context.Context, t test.Test, c cluster.Cluster, interval time.Duration,
) (*clusterStatCollector, error) {
	promCfg, cleanupFunc := setupPrometheus(ctx, t, c, nil, false, []workloadInstance{})
	prometheusNodeIP, err := c.ExternalIP(ctx, t.L(), promCfg.PrometheusNode)
	if err != nil {
		return nil, err
	}

	client, err := promapi.NewClient(promapi.Config{
		Address: fmt.Sprintf("http://%s:9090", prometheusNodeIP[0]),
	})
	if err != nil {
		return nil, err
	}

	return &clusterStatCollector{
		promClient: promv1.NewAPI(client),
		cleanup:    cleanupFunc,
		interval:   interval,
	}, nil
}

func (c *clusterStatCollector) scrapeStatInterval(
	ctx context.Context, q query, l *logger.Logger, fromTime time.Time, toTime time.Time,
) (model.Value, error) {
	// Add an extra interval to fromTime to account for the first data point
	// which may include a node not being fully shutdown or restarted.
	fromTime = fromTime.Add(prometheus.DefaultScrapeInterval)
	// Similarly, scale back the toTime to account for the data point
	// potentially already having data of a node which may have already
	// started restarting or shutting down.
	toTime = toTime.Add(-prometheus.DefaultScrapeInterval)
	if !toTime.After(fromTime) {
		l.PrintfCtx(
			ctx,
			"to %s < from %s, skipping",
			toTime.Format(time.RFC3339),
			fromTime.Format(time.RFC3339),
		)
		return nil, nil
	}

	r := promv1.Range{Start: fromTime, End: toTime, Step: c.interval}

	fromVal, warnings, err := c.promClient.QueryRange(ctx, string(q), r)
	if err != nil {
		return nil, err
	}
	if len(warnings) > 0 {
		return nil, errors.Newf("found warnings querying prometheus: %s", warnings)
	}

	return fromVal, nil
}

func (c *clusterStatCollector) scrapeStatPoint(
	ctx context.Context, l *logger.Logger, q query, at time.Time,
) (model.Value, error) {
	fromVal, warnings, err := c.promClient.Query(ctx, string(q), at)
	if err != nil {
		return nil, err
	}
	if len(warnings) > 0 {
		return nil, errors.Newf("found warnings querying prometheus: %s", warnings)
	}

	return fromVal, nil
}

type statSummaryQuery struct {
	stat     clusterStat
	aggQuery query
}

type StatSummary struct {
	Value  []float64
	Tagged map[tag][]float64
}

// TODO(kvoli): deserialize the values into the format we want then serialize
// them into the format for roachperf. The format we want should be general
// enough to be printed in the test log for debugging or use elsewhere.
// - [x] stat summary
// - [x] aggregate summaries into struct
// - [x] serialize summaries into roachperf fmt
// - [-] add roachperf parser for format
// - [ ] update roachperf rendering to adjust y axis in details (normalized between 1..100)
//
// - [-] update stream events to be tag:floats
func (cs *clusterStatCollector) finalize(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	fromTime time.Time,
	toTime time.Time,
	statQueries []statSummaryQuery,
	scalar func(summaries []StatSummary) float64,
) error {
	summaries := make([]StatSummary, 0, 1)
	for _, clusterStat := range statQueries {
		summary, err := cs.getStatSummary(ctx, t.L(), clusterStat, fromTime, toTime)
		if err != nil {
			return err
		}
		t.L().Printf("stat summary %s:  %+v", clusterStat.stat.tag, summary)
		summaries = append(summaries, summary)
	}

	perfBuf := serializeReport(summaries, scalar(summaries))

	dest := filepath.Join(t.PerfArtifactsDir(), "stats.json")
	if err := c.RunE(ctx, c.Node(1), "mkdir -p "+filepath.Dir(dest)); err != nil {
		t.L().ErrorfCtx(ctx, "failed to create perf dir: %+v", err)
	}
	if err := c.PutString(ctx, perfBuf.String(), dest, 0755, c.Node(1)); err != nil {
		t.L().ErrorfCtx(ctx, "failed to upload perf artifacts to node: %s", err.Error())
	}

	return nil
}

func serializeReport(summaries []StatSummary, summaryStat float64) *bytes.Buffer {
	// clusterStatsOP holds the aggregate and per tag information over a run for a
	// single stat, e.g. qps by store and sum of qps.

	// clusterStatsRun holds the summary value for a test run as well as per stat
	// information collected during the run.
	type ClusterStatRun struct {
		Total float64             `json:"total"`
		Stats map[tag]StatSummary `json:"stats"`
		Time  []int64             `json:"time"`
	}

	testRun := ClusterStatRun{Stats: make(map[tag]StatSummary)}

	for _, summary := range summaries {
		testRun.Stats[summary.Tag] = summary
	}
	testRun.Total = summaryStat

	bytesBuf := bytes.NewBuffer([]byte{})
	jsonEnc := json.NewEncoder(bytesBuf)
	jsonEnc.Encode(testRun)

	return bytesBuf
}

func (c *clusterStatCollector) getStatSummary(
	ctx context.Context,
	l *logger.Logger,
	summaryQuery statSummaryQuery,
	fromTime time.Time,
	toTime time.Time,
) (StatSummary, error) {
	ret := StatSummary{}

	fromValTagged, err := c.scrapeStatInterval(ctx, summaryQuery.stat.query, l, fromTime, toTime)
	if err != nil {
		return ret, err
	}
	fromValSummary, err := c.scrapeStatInterval(ctx, summaryQuery.aggQuery, l, fromTime, toTime)
	if err != nil {
		return ret, err
	}

	fromMatrixTagged := fromValTagged.(model.Matrix)
	fromMatrixSummary := fromValSummary.(model.Matrix)
	if len(fromMatrixTagged) == 0 {
		return ret, errors.Newf("unexpected empty fromMatrix for %s @ %s", summaryQuery.stat.query, fromTime.Format(time.RFC3339))
	}

	if len(fromMatrixSummary) != 1 {
		return ret, errors.Newf("unexpected fromMatrix for %s @ %s", summaryQuery.aggQuery, fromTime.Format(time.RFC3339))
	}

	ret.Tagged = make(map[tag][]float64)
	// fromMatrix is of the form []*SampleStream, where a sample stream is a tagged metric over time.
	// Here we assume that stats are collected per instance.
	for i, stream := range fromMatrixTagged {
		statTag := tag(stream.Metric["instance"])
		ret.Tagged[statTag] = make([]float64, len(stream.Values))

		// On the first metric, initialize the tick array to the correct size of samples.
		if i == 0 {
			ret.Time = make([]int64, len(stream.Values))
			ret.Value = make([]float64, len(stream.Values))
		}
		for j, val := range stream.Values {
			if i == 0 {
				ret.Time[j] = val.Timestamp.Unix()
			}
			ret.Tagged[statTag][j] = float64(val.Value)
		}
	}

	// The summary must have only one timeseries result.
	toMatrixSummary := fromMatrixSummary[0]
	ret.Value = make([]float64, len(toMatrixSummary.Values))
	for i, val := range toMatrixSummary.Values {
		ret.Value[i] = float64(val.Value)
	}

	ret.Tag = summaryQuery.stat.tag

	return ret, nil
}

type statEvent struct {
	tag     tag
	fromVal model.Value
}

type clusterStatStreamer struct {
	statCollector *clusterStatCollector
	processTickFn func(stats []statEvent) bool
	registered    map[tag]query
	errs          []error
}

func newClusterStatStreamer(
	statCollector *clusterStatCollector, processTickFn func(stats []statEvent) bool,
) *clusterStatStreamer {
	return &clusterStatStreamer{
		statCollector: statCollector,
		registered:    make(map[tag]query),
		errs:          make([]error, 0, 1),
		processTickFn: processTickFn,
	}
}

func (c *clusterStatStreamer) registerStat(stats ...clusterStat) {
	for _, stat := range stats {
		c.registered[stat.tag] = stat.query
	}
}

func (c *clusterStatStreamer) runStatStreamer(ctx context.Context, l *logger.Logger) {
	var statsTimer timeutil.Timer
	defer statsTimer.Stop()
	statsTimer.Reset(c.statCollector.interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-statsTimer.C:
			eventBuffer := make([]statEvent, 0, 1)
			statsTimer.Read = true
			queryTime := time.Now()
			for tag, q := range c.registered {
				fromVal, err := c.statCollector.scrapeStatPoint(ctx, l, q, queryTime)
				if err != nil {
					c.writeErr(ctx, l, err)
				}
				eventBuffer = append(eventBuffer, statEvent{tag: tag, fromVal: fromVal})
			}
			statsTimer.Reset(c.statCollector.interval)
			if finish := c.processTickFn(eventBuffer); finish {
				return
			}
		}
	}
}

func (ep *clusterStatStreamer) writeErr(ctx context.Context, l *logger.Logger, err error) {
	l.PrintfCtx(ctx, "error during stat streaming: %v", err)
	ep.errs = append(ep.errs, err)
}

func (ep *clusterStatStreamer) err() error {
	var err error
	for i := range ep.errs {
		if i == 0 {
			err = ep.errs[i]
		} else {
			err = errors.CombineErrors(err, ep.errs[i])
		}
	}
	return err
}
