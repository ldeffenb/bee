// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pusher provides protocol-orchestrating functionality
// over the pushsync protocol. It makes sure that chunks meant
// to be distributed over the network are sent used using the
// pushsync protocol.
package pusher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethersphere/bee/pkg/logging"
	"github.com/ethersphere/bee/pkg/pushsync"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/ethersphere/bee/pkg/tags"
	"github.com/ethersphere/bee/pkg/topology"
	"github.com/ethersphere/bee/pkg/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
)

type Service struct {
	storer            storage.Storer
	pushSyncer        pushsync.PushSyncer
	logger            logging.Logger
	tag               *tags.Tags
	tracer            *tracing.Tracer
	metrics           metrics
	quit              chan struct{}
	chunksWorkerQuitC chan struct{}

	retryMtx sync.Mutex
	retries map[string]Retry

}

type Retry struct {
	count int
	first time.Time
	last time.Time
}

var (
	retryInterval  = 5 * time.Second // time interval between retries
	concurrentJobs = 128              // how many chunks to push simultaneously
)

func New(storer storage.Storer, peerSuggester topology.ClosestPeerer, pushSyncer pushsync.PushSyncer, tagger *tags.Tags, logger logging.Logger, tracer *tracing.Tracer) *Service {
	service := &Service{
		storer:            storer,
		pushSyncer:        pushSyncer,
		tag:               tagger,
		logger:            logger,
		tracer:            tracer,
		metrics:           newMetrics(),
		quit:              make(chan struct{}),
		chunksWorkerQuitC: make(chan struct{}),

		retries:  make(map[string]Retry),
	}
	go service.chunksWorker()
	return service
}

// chunksWorker is a loop that keeps looking for chunks that are locally uploaded ( by monitoring pushIndex )
// and pushes them to the closest peer and get a receipt.
func (s *Service) chunksWorker() {
	var (
		chunks        <-chan swarm.Chunk
		unsubscribe   func()
		timer         = time.NewTimer(0) // timer, initially set to 0 to fall through select case on timer.C for initialisation
		chunksInBatch = -1
		cctx, cancel  = context.WithCancel(context.Background())
		ctx           = cctx
		sem           = make(chan struct{}, concurrentJobs)
		inflight      = make(map[string]struct{})
		mtx           sync.Mutex
		span          opentracing.Span
		logger        *logrus.Entry
	)
	defer timer.Stop()
	defer close(s.chunksWorkerQuitC)
	go func() {
		<-s.quit
		cancel()
	}()

LOOP:
	for {
		select {
		// handle incoming chunks
		case ch, more := <-chunks:
			// if no more, set to nil, reset timer to finalise batch
			if !more {
				chunks = nil
				var dur time.Duration
				if chunksInBatch == 0 {
					dur = 500 * time.Millisecond
				}
				timer.Reset(dur)
				s.logger.Tracef("pusher finished pushing %d chunks in batch, %d inflight", chunksInBatch, len(inflight))
				break
			}

			if span == nil {
				span, logger, ctx = s.tracer.StartSpanFromContext(cctx, "pusher-sync-batch", s.logger)
			}

			// postpone a retry only after we've finished processing everything in index
			timer.Reset(retryInterval)

			select {
			case sem <- struct{}{}:
			case <-s.quit:
				if unsubscribe != nil {
					unsubscribe()
				}
				if span != nil {
					span.Finish()
				}

				return
			}
			mtx.Lock()
			if _, ok := inflight[ch.Address().String()]; ok {
				mtx.Unlock()
				<-sem
				continue
			}

			inflight[ch.Address().String()] = struct{}{}
			mtx.Unlock()

			chunksInBatch++
			s.metrics.TotalToPush.Inc()

			go func(ctx context.Context, ch swarm.Chunk) {
				s.metrics.PusherConcurrency.Inc()
				defer s.metrics.PusherConcurrency.Dec()
				var (
					err       error
					startTime = time.Now()
					t         *tags.Tag
					setSent   bool
				)

		s.retryMtx.Lock()
		retry, ok := s.retries[ch.Address().String()]
		if !ok {
			retry = Retry{count: 0, first: time.Now(), last: time.Now() }
			s.retries[ch.Address().String()] = retry
		}
		s.retryMtx.Unlock()

				sentTo := "*Unknown*"
				var peer swarm.Address
				defer func() {
					if err == nil {
						s.metrics.TotalSynced.Inc()
						s.metrics.SyncTime.Observe(time.Since(startTime).Seconds())
						// only print this if there was no error while sending the chunk
						logger.Debugf("pusher pushed chunk %s in %d tries over %s to %s", ch.Address().String(), retry.count, time.Since(retry.first).Truncate(time.Millisecond), sentTo)
					} else {
						s.metrics.TotalErrors.Inc()
						s.metrics.ErrorTime.Observe(time.Since(startTime).Seconds())
						logger.Debugf("pusher chunk %s err %v", ch.Address().String(), err)
					}
					mtx.Lock()
					delete(inflight, ch.Address().String())
					mtx.Unlock()
					<-sem
				}()
				// Later when we process receipt, get the receipt and process it
				// for now ignoring the receipt and checking only for error
				_, peer, err = s.pushSyncer.PushChunkToClosest(ctx, ch)
				if err != nil {
					sentTo = fmt.Sprintf("err: %v", err)
					if errors.Is(err, topology.ErrWantSelf) {
						// we are the closest ones - this is fine
						// this is to make sure that the sent number does not diverge from the synced counter
						// the edge case is on the uploader node, in the case where the uploader node is
						// connected to other nodes, but is the closest one to the chunk.
						setSent = true
						err = nil	// Not really an error!
					} else {


		s.retryMtx.Lock()
//		var ok bool
//		retry, ok = s.retries[ch.Address().String()]
//		if !ok {
//			retry = Retry{count: 0, first: time.Now(), last: time.Now() }
//		}
		if errors.Is(err, topology.ErrNotFound) {	// Reset the first time on startup errors
			retry.first = time.Now()
		} else {	// Otherwise, count the retry
			retry.count++
			retry.last = time.Now()
		}
		if retry.count < 5 || time.Since(retry.first) < time.Duration(5)*time.Minute {
			s.retries[ch.Address().String()] = retry
			s.retryMtx.Unlock()
			logger.Debugf("pusher retried %d over %s pending chunk %s", retry.count, time.Since(retry.first).Truncate(time.Millisecond), ch.Address().String())
			return	// Keep trying this one
		}
		s.retries[ch.Address().String()] = retry
		s.retryMtx.Unlock()
		logger.Debugf("pusher retried %d over %s, keeping chunk %s", retry.count, time.Since(retry.first).Truncate(time.Millisecond), ch.Address().String())


					}
				} else {
					sentTo = peer.String()
				}

		s.retryMtx.Lock()
		delete(s.retries,ch.Address().String())
		s.retryMtx.Unlock()


				if err = s.storer.Set(ctx, storage.ModeSetSync, ch.Address()); err != nil {
					err = fmt.Errorf("pusher: set sync: %w", err)
					return
				}

				t, err = s.tag.Get(ch.TagID())
				if err == nil && t != nil {
					err = t.Inc(tags.StateSynced)
					if err != nil {
						err = fmt.Errorf("pusher: increment synced: %v", err)
						return
					}
					if setSent {
						err = t.Inc(tags.StateSent)
						if err != nil {
							err = fmt.Errorf("pusher: increment sent: %w", err)
							return
						}
					}
				} else {
					//if t == nil {
					//	logger.Tracef("pusher tagless chunk %s err %w", ch.Address().String(), err)
					//}
					err = nil	// Allow defer to TotalSynced.Inc() even if no tag
				}
			}(ctx, ch)
		case <-timer.C:
			// initially timer is set to go off as well as every time we hit the end of push index
			startTime := time.Now()

			// if subscribe was running, stop it
			if unsubscribe != nil {
				unsubscribe()
			}

			s.logger.Tracef("pusher timer.C after %d chunks in batch, %d inflight", chunksInBatch, len(inflight))

			chunksInBatch = 0

			// and start iterating on Push index from the beginning
			chunks, unsubscribe = s.storer.SubscribePush(ctx)

			// reset timer to go off after retryInterval
			timer.Reset(retryInterval)
			s.metrics.MarkAndSweepTime.Observe(time.Since(startTime).Seconds())

			if span != nil {
				span.Finish()
				span = nil
			}

		case <-s.quit:
			if unsubscribe != nil {
				unsubscribe()
			}
			if span != nil {
				span.Finish()
			}

			break LOOP
		}
	}

	// wait for all pending push operations to terminate
	closeC := make(chan struct{})
	go func() {
		defer func() { close(closeC) }()
		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}
	}()

	select {
	case <-closeC:
	case <-time.After(5 * time.Second):
		s.logger.Warning("pusher shutting down with pending operations")
	}
}

func (s *Service) Close() error {
	s.logger.Info("pusher shutting down")
	close(s.quit)

	// Wait for chunks worker to finish
	select {
	case <-s.chunksWorkerQuitC:
	case <-time.After(6 * time.Second):
	}
	return nil
}
