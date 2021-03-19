// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync

import (
	m "github.com/ethersphere/bee/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	OfferCounter    prometheus.Counter // number of chunks offered
	WantCounter     prometheus.Counter // number of chunks wanted
	DeliveryCounter prometheus.Counter // number of chunk deliveries
	DbOpsCounter    prometheus.Counter // number of db ops

	HandlerServiceCounter  prometheus.Counter // number of pullsync handler activations
	HandlerOfferCounter    prometheus.Counter // number of chunks offered by handler
	HandlerWantCounter     prometheus.Counter // number of chunks wanted by handler
	HandlerDeliveryCounter prometheus.Counter // number of chunk deliveries by handler
	HandlerDbOpsCounter    prometheus.Counter // number of db ops by handler
	HandlerConcurrency1    prometheus.Gauge // number of concurrent handler executions (before wait)
	HandlerConcurrency2    prometheus.Gauge // number of concurrent handler executions (after wait)
	HandlerConcurrency3    prometheus.Gauge // number of concurrent handler executions (after throttle)
	SyncConcurrency1    prometheus.Gauge // number of concurrent SyncInterval executions (before wait)
	SyncConcurrency2    prometheus.Gauge // number of concurrent SyncInterval executions (after wait)
	SyncConcurrency3    prometheus.Gauge // number of concurrent SyncInterval executions (after throttle)

	CursorDbOpsCounter    prometheus.Counter // number of db ops by cursors
}

func newMetrics() metrics {
	subsystem := "pullsync"

	return metrics{
		OfferCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunks_offered",
			Help:      "Total client chunks offered.",
		}),
		WantCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunks_wanted",
			Help:      "Total client chunks wanted.",
		}),
		DeliveryCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunks_delivered",
			Help:      "Total client chunks delivered.",
		}),
		DbOpsCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "db_ops",
			Help:      "Total client Db Ops.",
		}),
		HandlerServiceCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "pullsync_handled",
			Help:      "Total handler activations.",
		}),
		HandlerOfferCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "handler_chunks_offered",
			Help:      "Total handler chunks offered.",
		}),
		HandlerWantCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "handler_chunks_wanted",
			Help:      "Total handler chunks wanted.",
		}),
		HandlerDeliveryCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "handler_chunks_delivered",
			Help:      "Total handler chunks delivered.",
		}),
		HandlerDbOpsCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "handler_db_ops",
			Help:      "Total handler Db Ops.",
		}),
		HandlerConcurrency1: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "handler_concurrency_prewait",
			Help:      "Current handler concurrency pre-wait",
		}),
		HandlerConcurrency2: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "handler_concurrency_postwait",
			Help:      "Current handler concurrency post-wait",
		}),
		HandlerConcurrency3: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "handler_concurrency_throttled",
			Help:      "Current handler concurrency post-throttle",
		}),
		CursorDbOpsCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "cursor_db_ops",
			Help:      "Total cursor Db Ops.",
		}),
		SyncConcurrency1: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "sync_interval_concurrency_prewait",
			Help:      "Current SyncInterval concurrency pre-wait",
		}),
		SyncConcurrency2: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "sync_interval_concurrency_postwait",
			Help:      "Current SyncInterval concurrency post-wait",
		}),
		SyncConcurrency3: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "sync_interval_concurrency_throttled",
			Help:      "Current SyncInterval concurrency post-throttle",
		}),
	}
}

func (s *Syncer) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(s.metrics)
}
