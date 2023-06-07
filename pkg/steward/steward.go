// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package stewardess provides convenience methods
// for reseeding content on Swarm.
package steward

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/ethersphere/bee/pkg/log"
	"github.com/ethersphere/bee/pkg/pushsync"
	"github.com/ethersphere/bee/pkg/retrieval"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/ethersphere/bee/pkg/topology"
	"github.com/ethersphere/bee/pkg/traversal"
	"golang.org/x/sync/errgroup"
)

// how many parallel push operations
const parallelPush = 5

type Interface interface {
	// Reupload root hash and all of its underlying
	// associated chunks to the network.
	Reupload(context.Context, swarm.Address) error

	// IsRetrievable checks whether the content
	// on the given address is retrievable.
	IsRetrievable(context.Context, swarm.Address) (bool, error)

	Track(context.Context, swarm.Address) (bool, []*ChunkInfo, error)
}

type ChunkInfo struct {
	Address swarm.Address       `json:"address"`
	Batch string                `json:"batch"`
	PinCount uint64             `json:"pinCount"`
}

type steward struct {
	getter       storage.Getter
	logger       log.Logger
	push         pushsync.PushSyncer
	traverser    traversal.Traverser
	netTraverser traversal.Traverser
}

func New(getter storage.Getter, logger log.Logger, t traversal.Traverser, r retrieval.Interface, p pushsync.PushSyncer) Interface {
	return &steward{
		getter:       getter,
		logger:       logger,
		push:         p,
		traverser:    t,
		netTraverser: traversal.New(&netGetter{r}),
	}
}

// Reupload content with the given root hash to the network.
// The service will automatically dereference and traverse all
// addresses and push every chunk individually to the network.
// It assumes all chunks are available locally. It is therefore
// advisable to pin the content locally before trying to reupload it.
func (s *steward) Reupload(ctx context.Context, root swarm.Address) error {
	sem := make(chan struct{}, parallelPush)
	eg, _ := errgroup.WithContext(ctx)
	fn := func(addr swarm.Address) error {
		c, err := s.getter.Get(ctx, storage.ModeGetSync, addr)
		if err != nil {
			return err
		}

		sem <- struct{}{}
		eg.Go(func() error {
			defer func() { <-sem }()
			_, err := s.push.PushChunkToClosest(ctx, c)
			if err != nil {
				if !errors.Is(err, topology.ErrWantSelf) {
					for retry := 1; retry < 1; retry++ {
						_, err2 := s.push.PushChunkToClosest(ctx, c)
						if (err2 == nil || errors.Is(err2, topology.ErrWantSelf)) {
							return nil
						}
					}
					return err
				}
				// swallow the error in case we are the closest node
			}
			return nil
		})
		return nil
	}

	if err := s.traverser.Traverse(ctx, root, false, fn); err != nil {
		return fmt.Errorf("traversal of %s failed: %w", root.String(), err)
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("push error during reupload: %w", err)
	}
	return nil
}

// IsRetrievable implements Interface.IsRetrievable method.
func (s *steward) IsRetrievable(ctx context.Context, root swarm.Address) (bool, error) {
	noop := func(leaf swarm.Address) error { return nil }
	switch err := s.netTraverser.Traverse(ctx, root, false, noop); {
	case errors.Is(err, storage.ErrNotFound):
		return false, nil
	case errors.Is(err, topology.ErrNotFound):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("traversal of %q failed: %w", root, err)
	default:
		return true, nil
	}
}

// Track content with the given root hash to the network.
// The service will automatically dereference and traverse all
// addresses and push every chunk individually to the network.
// It assumes all chunks are available locally. It is therefore
// advisable to pin the content locally before trying to reupload it.
func (s *steward) Track(ctx context.Context, root swarm.Address) (bool, []*ChunkInfo, error) {

	var chunks []*ChunkInfo

	fn := func(addr swarm.Address) error {
		c, err := s.getter.Get(ctx, storage.ModeGetSync, addr)
		if err != nil {
			s.logger.Debug("steward:Track getter.Get failed", "chunk", addr, "err", err)
			return err
		}
		//stampBytes, err := c.Stamp().MarshalBinary()
		//if err != nil {
		//	return fmt.Errorf("pusher: valid stamp marshal: %w", err)
		//}
		batchID := hex.EncodeToString(c.Stamp().BatchID())
		checkFor := "0e8366a6fdac185b6f0327dc89af99e67d9d3b3f2af22432542dc5971065c1df"
		if (batchID != checkFor) {
			s.logger.Debug("steward:Track", "chunk", addr, "batchID", batchID, "not", checkFor)
		}
		pc, err := s.getter.PinCounter(addr)
		if err != nil {
			pc = 0
		}
		if pc == 0 {
			s.logger.Debug("steward:Track NOT pinned!", "chunk", addr, "batchID", batchID)
		}
		chunks = append(
			chunks,
			&ChunkInfo{
				Address: addr,
				Batch: batchID,
				PinCount: pc,
			},
		)
		return nil
	}

	if err := s.traverser.Traverse(ctx, root, false, fn); err != nil {
		return false, chunks, fmt.Errorf("traversal of %s failed: %w", root.String(), err)
	}

	return true, chunks, nil
}

// netGetter implements the storage Getter.Get method in a way
// that it will try to retrieve the chunk only from the network.
type netGetter struct {
	retrieval retrieval.Interface
}

func (ng *netGetter) PinCounter(addr swarm.Address) (uint64, error) {
	return 0, nil
}

// Get implements the storage Getter.Get interface.
func (ng *netGetter) Get(ctx context.Context, _ storage.ModeGet, addr swarm.Address) (swarm.Chunk, error) {
	return ng.retrieval.RetrieveChunk(ctx, addr, swarm.ZeroAddress)
}

// Put implements the storage Putter.Put interface.
func (ng *netGetter) Put(_ context.Context, _ storage.ModePut, _ ...swarm.Chunk) ([]bool, error) {
	return nil, errors.New("operation is not supported")
}
