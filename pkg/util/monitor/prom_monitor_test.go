// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package aggmetric provides functionality to create metrics which expose
// aggregate metrics for internal collection and additionally per-child
// reporting to prometheus.

package monitor

import (
	"bufio"
	"bytes"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

func TestPromMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := metric.NewRegistry()
	writePrometheusMetrics := func(t *testing.T) string {
		var in bytes.Buffer
		ex := metric.MakePrometheusExporter()
		ex.ScrapeRegistry(r, true /* includeChildMetrics */)
		require.NoError(t, ex.PrintAsText(&in))
		var lines []string
		for sc := bufio.NewScanner(&in); sc.Scan(); {
			if !bytes.HasPrefix(sc.Bytes(), []byte{'#'}) {
				lines = append(lines, sc.Text())
			}
		}
		sort.Strings(lines)
		return strings.Join(lines, "\n")
	}

	m := NewBaseMonitor()
	pm := NewPromMonitor(m, metric.Metadata{Name: "foo"})

	r.AddMetric(pm)

	m.Track("1")
	m.Track("2")
	m.Track("3")

	t.Run("export aggregate only by default", func(t *testing.T) {
		m.Report("1", 100)
		m.Report("2", 200)
		m.Report("3", 300)
		require.Equal(t,
			`foo 600`,
			writePrometheusMetrics(t))
	})

	t.Run("export children slots", func(t *testing.T) {
		pm.SetSlot(Label("1"), 1)
		pm.SetSlot(Label("3"), 3)
		require.Equal(t,
			`foo 600
foo{slot="1"} 100
foo{slot="3"} 300`,
			writePrometheusMetrics(t))
	})

	t.Run("remove slot stops export", func(t *testing.T) {
		pm.SetSlot(Label(""), 1)
		pm.SetSlot(Label("3"), 3)
		require.Equal(t,
			`foo 600
foo{slot="3"} 300`,
			writePrometheusMetrics(t))
	})
}
