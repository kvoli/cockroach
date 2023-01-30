// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metrics

import "context"

// TimeSeriesMetricListener implements the metrics Listen interface and is used
// for collecting timeseries data of a run.
type TimeSeriesMetricListener struct {
	collected *map[string][][]float64
}

// NewTimeSeriesMetricListener returns a new new TimeSeriesMetricListener.
func NewTimeSeriesMetricListener(
	collected *map[string][][]float64, stores int,
) *TimeSeriesMetricListener {
	tml := &TimeSeriesMetricListener{
		collected: collected,
	}
	(*tml.collected)["qps"] = make([][]float64, stores)
	(*tml.collected)["write"] = make([][]float64, stores)
	(*tml.collected)["write_b"] = make([][]float64, stores)
	(*tml.collected)["read"] = make([][]float64, stores)
	(*tml.collected)["read_b"] = make([][]float64, stores)
	(*tml.collected)["replicas"] = make([][]float64, stores)
	(*tml.collected)["leases"] = make([][]float64, stores)
	(*tml.collected)["lease_moves"] = make([][]float64, stores)
	(*tml.collected)["replica_moves"] = make([][]float64, stores)
	(*tml.collected)["replica_b_rcvd"] = make([][]float64, stores)
	(*tml.collected)["replica_b_sent"] = make([][]float64, stores)
	(*tml.collected)["range_splits"] = make([][]float64, stores)
	return tml
}

func MakeTS(metrics [][]StoreMetrics) map[string][][]float64 {
	ret := map[string][][]float64{}

	if len(metrics) == 0 || len(metrics[0]) == 0 {
		return ret
	}

	stores := len(metrics[0])
	ret["qps"] = make([][]float64, stores)
	ret["write"] = make([][]float64, stores)
	ret["write_b"] = make([][]float64, stores)
	ret["read"] = make([][]float64, stores)
	ret["read_b"] = make([][]float64, stores)
	ret["replicas"] = make([][]float64, stores)
	ret["leases"] = make([][]float64, stores)
	ret["lease_moves"] = make([][]float64, stores)
	ret["replica_moves"] = make([][]float64, stores)
	ret["replica_b_rcvd"] = make([][]float64, stores)
	ret["replica_b_sent"] = make([][]float64, stores)
	ret["range_splits"] = make([][]float64, stores)

	for _, sms := range metrics {

		for i, sm := range sms {
			ret["qps"][i] = append(ret["qps"][i], float64(sm.QPS))
			ret["write"][i] = append(ret["write"][i], float64(sm.WriteKeys))
			ret["write_b"][i] = append(ret["write_b"][i], float64(sm.WriteBytes))
			ret["read"][i] = append(ret["read"][i], float64(sm.ReadKeys))
			ret["read_b"][i] = append(ret["read_b"][i], float64(sm.ReadBytes))
			ret["replicas"][i] = append(ret["replicas"][i], float64(sm.Replicas))
			ret["leases"][i] = append(ret["leases"][i], float64(sm.Leases))
			ret["lease_moves"][i] = append(ret["lease_moves"][i], float64(sm.LeaseTransfers))
			ret["replica_moves"][i] = append(ret["replica_moves"][i], float64(sm.Rebalances))
			ret["replica_b_rcvd"][i] = append(ret["replica_b_rcvd"][i], float64(sm.RebalanceRcvdBytes))
			ret["replica_b_sent"][i] = append(ret["replica_b_sent"][i], float64(sm.RebalanceSentBytes))
			ret["range_splits"][i] = append(ret["range_splits"][i], float64(sm.RangeSplits))
		}
	}
	return ret
}

// s1 [t1,t2]    t1 [s1,s2,s3]
// s2 [t1,t2] -> t2 [s1,s2,s3]
// s3 [t1,t2]
func Transpose(mat [][]float64) [][]float64 {
	n := len(mat)
	var ret [][]float64
	if n < 1 {
		return ret
	}
	m := len(mat[0])
	if m < 1 {
		return ret
	}

	ret = make([][]float64, m)
	for j := 0; j < m; j++ {
		ret[j] = make([]float64, n)
		for i := 0; i < n; i++ {
			ret[j][i] = mat[i][j]
		}
	}
	return ret
}

// Listen implements the metrics tracker Listen interface.
func (tml *TimeSeriesMetricListener) Listen(ctx context.Context, sms []StoreMetrics) {
	for i, sm := range sms {
		(*tml.collected)["qps"][i] = append((*tml.collected)["qps"][i], float64(sm.QPS))
		(*tml.collected)["write"][i] = append((*tml.collected)["write"][i], float64(sm.WriteKeys))
		(*tml.collected)["write_b"][i] = append((*tml.collected)["write_b"][i], float64(sm.WriteBytes))
		(*tml.collected)["read"][i] = append((*tml.collected)["read"][i], float64(sm.ReadKeys))
		(*tml.collected)["read_b"][i] = append((*tml.collected)["read_b"][i], float64(sm.ReadBytes))
		(*tml.collected)["replicas"][i] = append((*tml.collected)["replicas"][i], float64(sm.Replicas))
		(*tml.collected)["leases"][i] = append((*tml.collected)["leases"][i], float64(sm.Leases))
		(*tml.collected)["lease_moves"][i] = append((*tml.collected)["lease_moves"][i], float64(sm.LeaseTransfers))
		(*tml.collected)["replica_moves"][i] = append((*tml.collected)["replica_moves"][i], float64(sm.Rebalances))
		(*tml.collected)["replica_b_rcvd"][i] = append((*tml.collected)["replica_b_rcvd"][i], float64(sm.RebalanceRcvdBytes))
		(*tml.collected)["replica_b_sent"][i] = append((*tml.collected)["replica_b_sent"][i], float64(sm.RebalanceSentBytes))
		(*tml.collected)["range_splits"][i] = append((*tml.collected)["range_splits"][i], float64(sm.RangeSplits))
	}
}
