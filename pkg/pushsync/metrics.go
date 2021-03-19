// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pushsync

import (
	m "github.com/ethersphere/bee/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	TotalSent     prometheus.Counter
	TotalReceived prometheus.Counter
	TotalErrors   prometheus.Counter
	HandlerConcurrency    prometheus.Gauge // number of concurrent handler executions
	ClosestConcurrency1    prometheus.Gauge // number of concurrent handler executions
	ClosestConcurrency2    prometheus.Gauge // number of concurrent handler executions
	DbConcurrency    prometheus.Gauge // number of concurrent handler executions
}

func newMetrics() metrics {
	subsystem := "pushsync"

	return metrics{
		TotalSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_sent",
			Help:      "Total chunks sent.",
		}),
		TotalReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_received",
			Help:      "Total chunks received.",
		}),
		TotalErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_errors",
			Help:      "Total no of time error received while sending chunk.",
		}),
		HandlerConcurrency: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "handler_concurrency",
			Help:      "Current handler concurrency",
		}),
		ClosestConcurrency1: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "closest_concurrency_prewait",
			Help:      "Current push-closest concurrency pre-wait",
		}),
		ClosestConcurrency2: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "closest_concurrency_postwait",
			Help:      "Current push-closest concurrency post-wait",
		}),
		DbConcurrency: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "db_put_concurrency",
			Help:      "Current db put concurrency",
		}),
	}
}

func (s *PushSync) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(s.metrics)
}
