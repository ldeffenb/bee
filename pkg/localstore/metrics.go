// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package localstore

import (
	m "github.com/ethersphere/bee/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	// all metrics fields must be exported
	// to be able to return them by Metrics()
	// using reflection

	TotalTimeCollectGarbage         prometheus.Counter
	TotalTimeGCExclude              prometheus.Counter
	TotalTimeGet                    prometheus.Counter
	TotalTimeUpdateGC               prometheus.Counter
	TotalTimeGetMulti               prometheus.Counter
	TotalTimeHas                    prometheus.Counter
	TotalTimeHasMulti               prometheus.Counter
	TotalTimePut                    prometheus.Counter
	TotalTimeSet                    prometheus.Counter
	TotalTimeSubscribePullIteration prometheus.Counter
	TotalTimeSubscribePushIteration prometheus.Counter
	
	TotalSyncRetrieve           prometheus.Counter
	TotalSyncBinID              prometheus.Counter
	TotalSyncRetrievalIndex     prometheus.Counter
	TotalSyncPullIndex          prometheus.Counter
	TotalSyncSetGC              prometheus.Counter
	TotalTimeSyncRetrieve           prometheus.Counter
	TotalTimeSyncBinID              prometheus.Counter
	TotalTimeSyncRetrievalIndex     prometheus.Counter
	TotalTimeSyncPullIndex          prometheus.Counter
	TotalTimeSyncSetGC              prometheus.Counter
	
	TotalForwardExists			   prometheus.Counter
	TotalForwardRePush			   prometheus.Counter
	TotalForwardPendPush		   prometheus.Counter
	TotalForwardSoonPush		   prometheus.Counter
	TotalForwardRetrieve           prometheus.Counter
	TotalForwardBinID              prometheus.Counter
	TotalForwardRetrievalIndex     prometheus.Counter
	TotalForwardPullIndex          prometheus.Counter
	TotalForwardPushIndex              prometheus.Counter
	TotalTimeForwardRetrieve           prometheus.Counter
	TotalTimeForwardBinID              prometheus.Counter
	TotalTimeForwardRetrievalIndex     prometheus.Counter
	TotalTimeForwardPullIndex          prometheus.Counter
	TotalTimeForwardPushIndex              prometheus.Counter
	
	BatchLockHitGC			prometheus.Counter
	BatchLockWaitTimeGC 	prometheus.Counter
	BatchLockHeldTimeGC 	prometheus.Counter
	BatchLockHitGet			prometheus.Counter
	BatchLockWaitTimeGet 	prometheus.Counter
	BatchLockHeldTimeGet 	prometheus.Counter
	BatchLockHitPut			prometheus.Counter
	BatchLockWaitTimePut 	prometheus.Counter
	BatchLockHeldTimePut 	prometheus.Counter
	BatchLockHitSet			prometheus.Counter
	BatchLockWaitTimeSet 	prometheus.Counter
	BatchLockHeldTimeSet 	prometheus.Counter
	
	GCCounter                prometheus.Counter
	GCErrorCounter           prometheus.Counter
	GCCollectedCounter       prometheus.Counter
	GCWriteBatchError        prometheus.Counter
	GCExcludeCounter         prometheus.Counter
	GCExcludeError           prometheus.Counter
	GCExcludedCounter        prometheus.Counter
	GCExcludeWriteBatchError prometheus.Counter
	GCUpdate                 prometheus.Counter
	GCUpdateError            prometheus.Counter

	ModeGet                       prometheus.Counter
	ModeGetFailure                prometheus.Counter
	ModeGetMulti                  prometheus.Counter
	ModeGetMultiFailure           prometheus.Counter
	ModePut                       prometheus.Counter
	ModePutFailure                prometheus.Counter
	ModeSet                       prometheus.Counter
	ModeSetFailure                prometheus.Counter
	ModeHas                       prometheus.Counter
	ModeHasFailure                prometheus.Counter
	ModeHasMulti                  prometheus.Counter
	ModeHasMultiFailure           prometheus.Counter
	SubscribePull                 prometheus.Counter
	SubscribePullStop             prometheus.Counter
	SubscribePullIteration        prometheus.Counter
	SubscribePullIterationFailure prometheus.Counter
	LastPullSubscriptionBinID     prometheus.Counter
	SubscribePush                 prometheus.Counter
	SubscribePushIteration        prometheus.Counter
	SubscribePushIterationDone    prometheus.Counter
	SubscribePushIterationFailure prometheus.Counter

	GCSize                  prometheus.Gauge
	GCStoreTimeStamps       prometheus.Gauge
	GCStoreAccessTimeStamps prometheus.Gauge
}

func newMetrics() metrics {
	subsystem := "localstore"

	return metrics{
		TotalTimeCollectGarbage: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_time",
			Help:      "Total time taken to collect garbage.",
		}),
		TotalTimeGCExclude: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_exclude_index_time",
			Help:      "Total time taken to exclude gc index.",
		}),
		TotalTimeGet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "get_chunk_time",
			Help:      "Total time taken to get chunk from DB.",
		}),
		TotalTimeUpdateGC: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "in_gc_time",
			Help:      "Total time taken to in gc.",
		}),
		TotalTimeGetMulti: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "get_multi_time",
			Help:      "Total time taken to get multiple chunks from DB.",
		}),
		TotalTimeHas: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "has_time",
			Help:      "Total time taken to check if the key is present in DB.",
		}),
		TotalTimeHasMulti: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "has_multi_time",
			Help:      "Total time taken to check if multiple keys are present in DB.",
		}),
		TotalTimePut: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "put_time",
			Help:      "Total time taken to put a chunk in DB.",
		}),
		TotalTimeSet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "set_time",
			Help:      "Total time taken to set chunk in DB.",
		}),
		TotalTimeSubscribePullIteration: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "subscribe_pull_iteration_time",
			Help:      "Total time taken to subsctibe for pull iteration.",
		}),
		TotalTimeSubscribePushIteration: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "subscribe_push_iteration_time",
			Help:      "Total time taken to subscribe for push iteration.",
		}),


		
		TotalSyncRetrieve: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_sync_retrieve",
			Help:      "Number of times retrievalDataIndex.Has is invoked.",
		}),
		TotalTimeSyncRetrieve: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_sync_retrieve_time",
			Help:      "Total time taken by retrievalDataIndex.Has",
		}),
		TotalSyncBinID: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_sync_bin_id",
			Help:      "Number of times incBinID is invoked.",
		}),
		TotalTimeSyncBinID: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_sync_bin_id_time",
			Help:      "Total time taken by incBinID",
		}),
		TotalSyncRetrievalIndex: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_sync_retrieval_index",
			Help:      "Number of times retrievalDataIndex.PutInBatch is invoked.",
		}),
		TotalTimeSyncRetrievalIndex: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_sync_retrieval_index_time",
			Help:      "Total time taken by retrievalDataIndex.PutInBatch",
		}),
		TotalSyncPullIndex: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_sync_pull_index",
			Help:      "Number of times pullIndex.PutInBatch is invoked.",
		}),
		TotalTimeSyncPullIndex: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_sync_pull_index_time",
			Help:      "Total time taken by rpullIndex.PutInBatch",
		}),
		TotalSyncSetGC: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_sync_set_gc",
			Help:      "Number of times setGC is invoked.",
		}),
		TotalTimeSyncSetGC: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_sync_set_gc_time",
			Help:      "Total time taken by setGC",
		}),




		TotalForwardExists: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_forward_exists",
			Help:      "Number of items forwarded which already exist.",
		}),
		TotalForwardRePush: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_forward_re_push",
			Help:      "Number of existing items added back to pushIndex.",
		}),
		TotalForwardPendPush: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_forward_pend_push",
			Help:      "Number of existing items still in pushIndex.",
		}),
		TotalForwardSoonPush: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_forward_soon_push",
			Help:      "Number of existing items too new to re-pushIndex.",
		}),
		TotalForwardRetrieve: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_forward_retrieve",
			Help:      "Number of times retrievalDataIndex.Has is invoked.",
		}),
		TotalTimeForwardRetrieve: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_forward_retrieve_time",
			Help:      "Total time taken by retrievalDataIndex.Has",
		}),
		TotalForwardBinID: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_forward_bin_id",
			Help:      "Number of times incBinID is invoked.",
		}),
		TotalTimeForwardBinID: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_forward_bin_id_time",
			Help:      "Total time taken by incBinID",
		}),
		TotalForwardRetrievalIndex: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_forward_retrieval_index",
			Help:      "Number of times retrievalDataIndex.PutInBatch is invoked.",
		}),
		TotalTimeForwardRetrievalIndex: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_forward_retrieval_index_time",
			Help:      "Total time taken by retrievalDataIndex.PutInBatch",
		}),
		TotalForwardPullIndex: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_forward_pull_index",
			Help:      "Number of times pullIndex.PutInBatch is invoked.",
		}),
		TotalTimeForwardPullIndex: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_forward_pull_index_time",
			Help:      "Total time taken by pullIndex.PutInBatch",
		}),
		TotalForwardPushIndex: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_forward_push_index",
			Help:      "Number of times pushIndex.PutInBatch is invoked.",
		}),
		TotalTimeForwardPushIndex: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_sync_push_index_time",
			Help:      "Total time taken by pushIndex.PutInBatch",
		}),



		
		BatchLockHitGC: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "batch_lock_hit_gc",
			Help:      "Number of gc batchMU locks.",
		}),
		BatchLockWaitTimeGC: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "batch_lock_wait_gc",
			Help:      "Total time gc waited for batchMU lock",
		}),
		BatchLockHeldTimeGC: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "batch_lock_held_gc",
			Help:      "Total time gc held batchMU lock",
		}),

		BatchLockHitGet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "batch_lock_hit_get",
			Help:      "Number of get batchMU locks.",
		}),
		BatchLockWaitTimeGet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "batch_lock_wait_get",
			Help:      "Total time get waited for batchMU lock",
		}),
		BatchLockHeldTimeGet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "batch_lock_held_get",
			Help:      "Total time get held batchMU lock",
		}),

		BatchLockHitPut: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "batch_lock_hit_put",
			Help:      "Number of put batchMU locks.",
		}),
		BatchLockWaitTimePut: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "batch_lock_wait_put",
			Help:      "Total time put waited for batchMU lock",
		}),
		BatchLockHeldTimePut: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "batch_lock_held_put",
			Help:      "Total time put held batchMU lock",
		}),

		BatchLockHitSet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "batch_lock_hit_set",
			Help:      "Number of set batchMU locks.",
		}),
		BatchLockWaitTimeSet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "batch_lock_wait_set",
			Help:      "Total time set waited for batchMU lock",
		}),
		BatchLockHeldTimeSet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "batch_lock_held_set",
			Help:      "Total time set held batchMU lock",
		}),
		
		
		GCCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_count",
			Help:      "Number of times the GC operation is done.",
		}),
		GCErrorCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_fail_count",
			Help:      "Number of times the GC operation failed.",
		}),
		GCCollectedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_collected_count",
			Help:      "Number of times the GC_COLLECTED operation is done.",
		}),
		GCWriteBatchError: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_write_batch_error_count",
			Help:      "Number of times the GC_WRITE_BATCH operation failed.",
		}),
		GCExcludeCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_exclude_count",
			Help:      "Number of times the GC_EXCLUDE operation is done.",
		}),
		GCExcludeError: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_exclude_fail_count",
			Help:      "Number of times the GC_EXCLUDE operation failed.",
		}),
		GCExcludedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_excluded_count",
			Help:      "Number of times the GC_EXCLUDED operation is done.",
		}),
		GCExcludeWriteBatchError: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "ex_exclude_write_batch_fail_count",
			Help:      "Number of times the GC_EXCLUDE_WRITE_BATCH operation is failed.",
		}),
		GCUpdate: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_update_count",
			Help:      "Number of times the gc is updated.",
		}),
		GCUpdateError: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "fc_update_error_count",
			Help:      "Number of times the gc update had error.",
		}),

		ModeGet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_get_count",
			Help:      "Number of times MODE_GET is invoked.",
		}),
		ModeGetFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_get_failure_count",
			Help:      "Number of times MODE_GET invocation failed.",
		}),
		ModeGetMulti: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_get_multi_count",
			Help:      "Number of times MODE_MULTI_GET is invoked.",
		}),
		ModeGetMultiFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_get_multi_failure_count",
			Help:      "Number of times MODE_GET invocation failed.",
		}),
		ModePut: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_put_count",
			Help:      "Number of times MODE_PUT is invoked.",
		}),
		ModePutFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_put_failure_count",
			Help:      "Number of times MODE_PUT invocation failed.",
		}),
		ModeSet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_set_count",
			Help:      "Number of times MODE_SET is invoked.",
		}),
		ModeSetFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_set_failure_count",
			Help:      "Number of times MODE_SET invocation failed.",
		}),
		ModeHas: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_has_count",
			Help:      "Number of times MODE_HAS is invoked.",
		}),
		ModeHasFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_has_failure_count",
			Help:      "Number of times MODE_HAS invocation failed.",
		}),
		ModeHasMulti: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_has_multi_count",
			Help:      "Number of times MODE_HAS_MULTI is invoked.",
		}),
		ModeHasMultiFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "mode_has_multi_failure_count",
			Help:      "Number of times MODE_HAS_MULTI invocation failed.",
		}),
		SubscribePull: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "subscribe_pull_count",
			Help:      "Number of times Subscribe_pULL is invoked.",
		}),
		SubscribePullStop: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "subscribe_pull_stop_count",
			Help:      "Number of times Subscribe_pull_stop is invoked.",
		}),
		SubscribePullIteration: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "subscribe_pull_iteration_count",
			Help:      "Number of times Subscribe_pull_iteration is invoked.",
		}),
		SubscribePullIterationFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "subscribe_pull_iteration_fail_count",
			Help:      "Number of times Subscribe_pull_iteration_fail is invoked.",
		}),
		LastPullSubscriptionBinID: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "last_pull_subscription_bin_id_count",
			Help:      "Number of times LastPullSubscriptionBinID is invoked.",
		}),
		SubscribePush: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "subscribe_push_count",
			Help:      "Number of times SUBSCRIBE_PUSH is invoked.",
		}),
		SubscribePushIteration: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "subscribe_push_iteration_count",
			Help:      "Number of times SUBSCRIBE_PUSH_ITERATION is invoked.",
		}),
		SubscribePushIterationDone: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "subscribe_push_iteration_done_count",
			Help:      "Number of times SUBSCRIBE_PUSH_ITERATION_DONE is invoked.",
		}),
		SubscribePushIterationFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "subscribe_push_iteration_failure_count",
			Help:      "Number of times SUBSCRIBE_PUSH_ITERATION_FAILURE is invoked.",
		}),

		GCSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_size",
			Help:      "Number of elements in Garbage collection index.",
		}),
		GCStoreTimeStamps: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_time_stamp",
			Help:      "Storage timestamp in Garbage collection iteration.",
		}),
		GCStoreAccessTimeStamps: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "gc_access_time_stamp",
			Help:      "Access timestamp in Garbage collection iteration.",
		}),
	}
}

func (s *DB) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(s.metrics)
}
