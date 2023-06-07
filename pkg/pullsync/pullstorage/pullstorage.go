// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullstorage

import (
	"context"
	"errors"
	"fmt"
//	"math"
//	"runtime"
	"sync/atomic"
	"time"

	"github.com/ethersphere/bee/pkg/log"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
	"golang.org/x/sync/semaphore"
	"resenje.org/singleflight"
)

const loggerName = "pullstorage"

var (
	_ Storer = (*PullStorer)(nil)
	// ErrDbClosed is used to signal the underlying database was closed
	ErrDbClosed = errors.New("db closed")

	// after how long to return a non-empty batch
	batchTimeout = 500 * time.Millisecond
)

// Storer is a thin wrapper around storage.Storer.
// It is used in order to collect and provide information about chunks
// currently present in the local store.
type Storer interface {
	// IntervalChunks collects chunk for a requested interval.
	IntervalChunks(ctx context.Context, bin uint8, from, to uint64, limit int) (chunks []swarm.Address, topmost uint64, err error)
	// Cursors gets the last BinID for every bin in the local storage
	Cursors(ctx context.Context) ([]uint64, error)
	// Get chunks.
	Get(ctx context.Context, mode storage.ModeGet, addrs ...swarm.Address) ([]swarm.Chunk, error)
	// Put chunks.
	Put(ctx context.Context, mode storage.ModePut, chs ...swarm.Chunk) error
	// Set chunks.
	Set(ctx context.Context, mode storage.ModeSet, addrs ...swarm.Address) error
	// Has chunks.
	Has(ctx context.Context, addr swarm.Address) (bool, error)
}

// PullStorer wraps storage.Storer.
type PullStorer struct {
	storage.Storer
	intervalsSF singleflight.Group
	cursorsSF   singleflight.Group
	sfCount	    atomic.Int64 
	logger      log.Logger
        cursorSem   *semaphore.Weighted
	metrics     metrics
}

// New returns a new pullstorage Storer instance.
func New(storer storage.Storer, logger log.Logger) *PullStorer {
	return &PullStorer{
		Storer:  storer,
                //cursorSem: semaphore.NewWeighted(int64(math.Max(1,float64(runtime.NumCPU()/2)))),
		cursorSem: semaphore.NewWeighted(1),	// Extreme restrictions!
		metrics: newMetrics(),
		logger:  logger.WithName(loggerName).Register(),
	}
}

// IntervalChunks collects chunk for a requested interval.
func (s *PullStorer) IntervalChunks(ctx context.Context, bin uint8, from, to uint64, limit int) ([]swarm.Address, uint64, error) {
	loggerV2 := s.logger.V(2).Register()

	type result struct {
		chs     []swarm.Address
		topmost uint64
	}
	s.metrics.TotalSubscribePullRequests.Inc()
	defer s.metrics.TotalSubscribePullRequestsComplete.Inc()

	v, shared, err := s.intervalsSF.Do(ctx, fmt.Sprintf("%v-%v-%v-%v", bin, from, to, limit), func(ctx context.Context) (interface{}, error) {
		var (
			chs     []swarm.Address
			topmost uint64
		)
		// call iterator, iterate either until upper bound or limit reached
		// return addresses, topmost is the topmost bin ID
		var (
			timer  *time.Timer
			timerC <-chan time.Time
		)
		s.metrics.SubscribePullsStarted.Inc()
		ch, dbClosed, stop := s.SubscribePull(ctx, bin, from, to)
		defer func(start time.Time) {
			stop()
			if timer != nil {
				timer.Stop()
			}
			s.metrics.SubscribePullsComplete.Inc()
		}(time.Now())

		var nomore bool

	LOOP:
		for limit > 0 {
			select {
			case v, ok := <-ch:
				if !ok {
					nomore = true
					break LOOP
				}
				chs = append(chs, v.Address)
				if v.BinID > topmost {
					topmost = v.BinID
				}
				limit--
				if timer == nil {
					timer = time.NewTimer(batchTimeout)
				} else {
					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(batchTimeout)
				}
				timerC = timer.C
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-timerC:
				loggerV2.Debug("batch timeout timer triggered")
				// return batch if new chunks are not received after some time
				break LOOP
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-dbClosed:
			return nil, ErrDbClosed
		default:
		}

		if nomore {
			// end of interval reached. no more chunks so interval is complete
			// return requested `to`. it could be that len(chs) == 0 if the interval
			// is empty
			loggerV2.Debug("no more batches from the subscription", "to", to, "topmost", topmost)
			topmost = to
		}

		return &result{chs: chs, topmost: topmost}, nil
	})

	if err != nil {
		s.metrics.SubscribePullsFailures.Inc()
		return nil, 0, err
	}
	if shared {
		s.logger.Debug("pullstorage:IntervalChunks:shared", "key", fmt.Sprintf("%v-%v-%v-%v", bin, from, to, limit))
	}
	r := v.(*result)
	return r.chs, r.topmost, nil
}

// Cursors gets the last BinID for every bin in the local storage
func (s *PullStorer) Cursors(ctx context.Context) ([]uint64, error) {

        type result struct {
                curs     []uint64
        }

      s.metrics.TotalCursorsRequests.Inc()
      defer s.metrics.TotalCursorsRequestsComplete.Inc()

        v, shared, err := s.intervalsSF.Do(ctx, "CursorsKey", func(ctx context.Context) (interface{}, error) {
      id := s.sfCount.Add(1)
      s.logger.Debug("pullstorage:Cursors:starting singleflight", "id", id)
      curs := make([]uint64, swarm.MaxBins)
      for i := uint8(0); i < swarm.MaxBins; i++ {
	      if ctx.Err() != nil {
		s.logger.Debug("pullstorage:cursors:singleflight:ctx.Err", "id", id, "err", ctx.Err())
		return nil, ctx.Err()
	      }
              if err := s.cursorSem.Acquire(ctx, 1); err != nil {
                      s.metrics.PullCursorsFailures.Inc()
			s.logger.Debug("pullstorage:cursors:singleflight:cursorSem.Acquire", "id", id, "err", err)
                      return nil, err
              }
              start := time.Now()
              s.metrics.PullCursorsStarted.Inc()
              binID, err := s.Storer.LastPullSubscriptionBinID(i)
              if err != nil {
                      s.metrics.PullCursorsFailures.Inc()
                      s.cursorSem.Release(1)
			s.logger.Debug("pullstorage:cursors:singleflight:LastPull", "id", id, "err", err)
                      return nil, err
              }
              curs[i] = binID
              s.metrics.PullCursorsComplete.Inc()
              s.cursorSem.Release(1)
	      if time.Since(start) > time.Second {
		s.logger.Debug("pullstorage:Cursors:bin complete", "id", id, "bin", i, "elapsed", time.Since(start), "last", binID)
	      }
      }
      s.logger.Debug("pullstorage:Cursors:finish singleflight", "id", id)
      //return curs, nil
      return &result{curs: curs}, nil
        })

        if err != nil {
s.logger.Debug("pullstorage:Cursors:err", "err", err, "shared", shared)
                return nil, err
        }
        if shared {
                s.logger.Debug("pullstorage:Cursors:shared!")
        }
        r := v.(*result)
        return r.curs, nil

//	curs = make([]uint64, swarm.MaxBins)
//	for i := uint8(0); i < swarm.MaxBins; i++ {
//		binID, err := s.Storer.LastPullSubscriptionBinID(i)
//		if err != nil {
//			return nil, err
//		}
//		curs[i] = binID
//	}
//	return curs, nil
}

// Get chunks.
func (s *PullStorer) Get(ctx context.Context, mode storage.ModeGet, addrs ...swarm.Address) ([]swarm.Chunk, error) {
	return s.Storer.GetMulti(ctx, mode, addrs...)
}

// Put chunks.
func (s *PullStorer) Put(ctx context.Context, mode storage.ModePut, chs ...swarm.Chunk) error {
	_, err := s.Storer.Put(ctx, mode, chs...)
	return err
}

// Put chunks.
func (s *PullStorer) Has(ctx context.Context, addr swarm.Address) (bool, error) {
	rstore, ok := s.Storer.(storage.ReserveHasser)
	if ok {
		return rstore.HasReserve(ctx, addr)
	}
	return s.Storer.Has(ctx, addr)
}
