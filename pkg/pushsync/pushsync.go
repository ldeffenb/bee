// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pushsync provides the pushsync protocol
// implementation.
package pushsync

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ethersphere/bee/v2/pkg/accounting"
	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/protobuf"
	"github.com/ethersphere/bee/v2/pkg/postage"
	"github.com/ethersphere/bee/v2/pkg/pricer"
	"github.com/ethersphere/bee/v2/pkg/pushsync/pb"
	"github.com/ethersphere/bee/v2/pkg/skippeers"
	"github.com/ethersphere/bee/v2/pkg/soc"
	storage "github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology"
	"github.com/ethersphere/bee/v2/pkg/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	olog "github.com/opentracing/opentracing-go/log"
)

// loggerName is the tree path name of the logger for this package.
const loggerName = "pushsync"

const (
	protocolName    = "pushsync"
	protocolVersion = "1.3.0"
	streamName      = "pushsync"
)

const (
	defaultTTL         = 30 * time.Second // request time to live
	preemptiveInterval = 5 * time.Second  // P90 request time to live
	sanctionWait       = 5 * time.Minute
	overDraftRefresh   = time.Millisecond * 600
)

const (
	maxMultiplexForwards = 2 // number of extra peers to forward the request from the multiplex node
	maxPushErrors        = 32
)

var (
	ErrNoPush            = errors.New("could not push chunk")
	ErrOutOfDepthStoring = errors.New("storing outside of the neighborhood")
	ErrWarmup            = errors.New("node warmup time not complete")
)

type PushSyncer interface {
	PushChunkToClosest(ctx context.Context, ch swarm.Chunk) (*Receipt, error)
}

type Receipt struct {
	Address   swarm.Address
	Signature []byte
	Nonce     []byte
}

type Storer interface {
	storage.PushReporter
	ReservePutter() storage.Putter
	IsWithinStorageRadius(swarm.Address) bool
	StorageRadius() uint8
}

type PushSync struct {
	address        swarm.Address
	nonce          []byte
	streamer       p2p.StreamerDisconnecter
	store          Storer
	topologyDriver topology.Driver
	unwrap         func(swarm.Chunk)
	logger         log.Logger
	accounting     accounting.Interface
	pricer         pricer.Interface
	metrics        metrics
	tracer         *tracing.Tracer
	validStamp     postage.ValidStampFn
	signer         crypto.Signer
	fullNode       bool
	skipList       *skippeers.List
	warmupPeriod   time.Time
}

type receiptResult struct {
	pushTime time.Time
	peer     swarm.Address
	receipt  *pb.Receipt
	err      error
}

func New(
	address swarm.Address,
	nonce []byte,
	streamer p2p.StreamerDisconnecter,
	store Storer,
	topology topology.Driver,
	fullNode bool,
	unwrap func(swarm.Chunk),
	validStamp postage.ValidStampFn,
	logger log.Logger,
	accounting accounting.Interface,
	pricer pricer.Interface,
	signer crypto.Signer,
	tracer *tracing.Tracer,
	warmupTime time.Duration,
) *PushSync {
	ps := &PushSync{
		address:        address,
		nonce:          nonce,
		streamer:       streamer,
		store:          store,
		topologyDriver: topology,
		fullNode:       fullNode,
		unwrap:         unwrap,
		logger:         logger.WithName(loggerName).Register(),
		accounting:     accounting,
		pricer:         pricer,
		metrics:        newMetrics(),
		tracer:         tracer,
		signer:         signer,
		skipList:       skippeers.NewList(),
		warmupPeriod:   time.Now().Add(warmupTime),
	}

	ps.validStamp = ps.validStampWrapper(validStamp)
	return ps
}

func (s *PushSync) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamName,
				Handler: s.handler,
			},
		},
	}
}

// handler handles chunk delivery from other node and forwards to its destination node.
// If the current node is the destination, it stores in the local store and sends a receipt.
func (ps *PushSync) handler(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	now := time.Now()

	w, r := protobuf.NewWriterAndReader(stream)
	var attemptedWrite bool

	ctx, cancel := context.WithTimeout(ctx, defaultTTL)
	defer cancel()

	defer func() {
		if err != nil {
			ps.metrics.TotalHandlerTime.WithLabelValues("failure").Observe(time.Since(now).Seconds())
			ps.metrics.TotalHandlerErrors.Inc()
			if !attemptedWrite {
				_ = w.WriteMsgWithContext(ctx, &pb.Receipt{Err: err.Error()})
			}
			_ = stream.Reset()
		} else {
			ps.metrics.TotalHandlerTime.WithLabelValues("success").Observe(time.Since(now).Seconds())
			_ = stream.FullClose()
		}
	}()

	var ch pb.Delivery
	if err = r.ReadMsgWithContext(ctx, &ch); err != nil {
		return fmt.Errorf("pushsync read delivery: %w", err)
	}

	ps.metrics.TotalReceived.Inc()

	chunk := swarm.NewChunk(swarm.NewAddress(ch.Address), ch.Data)
	chunkAddress := chunk.Address()

	span, _, ctx := ps.tracer.StartSpanFromContext(ctx, "pushsync-handler", ps.logger, opentracing.Tag{Key: "address", Value: chunkAddress.String()}, opentracing.Tag{Key: "tagID", Value: chunk.TagID()}, opentracing.Tag{Key: "sender_address", Value: p.Address.String()})

	var (
		stored bool
		reason string
	)

	defer func() {
		if err != nil {
			ext.LogError(span, err)
		} else {
			var logs []olog.Field
			logs = append(logs, olog.Bool("success", true))
			if stored {
				logs = append(logs, olog.Bool("stored", true))
				logs = append(logs, olog.String("reason", reason))
			}
			span.LogFields(logs...)
		}
		span.Finish()
	}()

	stamp := new(postage.Stamp)
	err = stamp.UnmarshalBinary(ch.Stamp)
	if err != nil {
		return fmt.Errorf("pushsync stamp unmarshall: %w", err)
	}
	chunk.WithStamp(stamp)

	if cac.Valid(chunk) {
		go ps.unwrap(chunk)
	} else if !soc.Valid(chunk) {
		return swarm.ErrInvalidChunk
	}

	price := ps.pricer.Price(chunkAddress)

	store := func(ctx context.Context) error {
		ps.metrics.Storer.Inc()

		chunkToPut, err := ps.validStamp(chunk)
		if err != nil {
			return fmt.Errorf("invalid stamp: %w", err)
		}

		err = ps.store.ReservePutter().Put(ctx, chunkToPut)
		if err != nil {
			return fmt.Errorf("reserve put: %w", err)
		}

		signature, err := ps.signer.Sign(chunkToPut.Address().Bytes())
		if err != nil {
			return fmt.Errorf("receipt signature: %w", err)
		}

		// return back receipt
		debit, err := ps.accounting.PrepareDebit(ctx, p.Address, price)
		if err != nil {
			return fmt.Errorf("prepare debit to peer %s before writeback: %w", p.Address.String(), err)
		}
		defer debit.Cleanup()

		attemptedWrite = true

		receipt := pb.Receipt{Address: chunkToPut.Address().Bytes(), Signature: signature, Nonce: ps.nonce}
		if err := w.WriteMsgWithContext(ctx, &receipt); err != nil {
			return fmt.Errorf("send receipt to peer %s: %w", p.Address.String(), err)
		}

		return debit.Apply()
	}

	if ps.topologyDriver.IsReachable() && ps.store.IsWithinStorageRadius(chunkAddress) {
		stored, reason = true, "is within AOR"
		return store(ctx)
	}

	receipt, err := ps.pushToClosest(ctx, chunk, false)
	if err != nil {
		if errors.Is(err, topology.ErrWantSelf) {
			stored, reason = true, "want self"
			return store(ctx)
		}

		ps.metrics.Forwarder.Inc()
		return fmt.Errorf("handler: push to closest chunk %s: %w", chunkAddress, err)
	}

	ps.metrics.Forwarder.Inc()

	debit, err := ps.accounting.PrepareDebit(ctx, p.Address, price)
	if err != nil {
		return fmt.Errorf("prepare debit to peer %s before writeback: %w", p.Address.String(), err)
	}
	defer debit.Cleanup()

	attemptedWrite = true

	// pass back the receipt
	if err := w.WriteMsgWithContext(ctx, receipt); err != nil {
		return fmt.Errorf("send receipt to peer %s: %w", p.Address.String(), err)
	}

	return debit.Apply()
}

// PushChunkToClosest sends chunk to the closest peer by opening a stream. It then waits for
// a receipt from that peer and returns error or nil based on the receiving and
// the validity of the receipt.
func (ps *PushSync) PushChunkToClosest(ctx context.Context, ch swarm.Chunk) (*Receipt, error) {
	ps.metrics.TotalOutgoing.Inc()
	r, err := ps.pushToClosest(ctx, ch, true)
	if err != nil {
		ps.metrics.TotalOutgoingErrors.Inc()
		return nil, err
	}
	return &Receipt{
		Address:   swarm.NewAddress(r.Address),
		Signature: r.Signature,
		Nonce:     r.Nonce,
	}, nil
}

// pushToClosest attempts to push the chunk into the network.
func (ps *PushSync) pushToClosest(ctx context.Context, ch swarm.Chunk, origin bool) (*pb.Receipt, error) {

	if !ps.warmedUp() {
		return nil, ErrWarmup
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ps.metrics.TotalRequests.Inc()

	var (
		sentErrorsLeft   = 1
		preemptiveTicker <-chan time.Time
		inflight         int
		parallelForwards = maxMultiplexForwards
	)

	if origin {
		ticker := time.NewTicker(preemptiveInterval)
		defer ticker.Stop()
		preemptiveTicker = ticker.C
		sentErrorsLeft = maxPushErrors
	}

	resultChan := make(chan receiptResult)

	retryC := make(chan struct{}, parallelForwards)

	retry := func(why string) {
		ps.logger.Debug("pushTrace:pushToClosest:retry", "chunk", ch.Address(), "why", why)
		select {
		case retryC <- struct{}{}:
		case <-ctx.Done():
		default:
		}
	}

	retry("initial")

	for sentErrorsLeft > 0 {
		select {
		case <-ctx.Done():
			return nil, ErrNoPush
		case <-preemptiveTicker:
			retry("preemptiveTicker")
		case <-retryC:

			// Origin peers should not store the chunk initially so that the chunk is always forwarded into the network.
			// If no peer can be found from an origin peer, the origin peer may store the chunk.
			// Non-origin peers store the chunk if the chunk is within depth.
			// For non-origin peers, if the chunk is not within depth, they may store the chunk if they are the closest peer to the chunk.
			peer, err := ps.closestPeer(ch.Address(), origin)
			if errors.Is(err, topology.ErrNotFound) {
				if ps.skipList.PruneExpiresAfter(ch.Address(), overDraftRefresh) == 0 { //no overdraft peers, we have depleted ALL peers
					if inflight == 0 {
						if ps.fullNode {
							if cac.Valid(ch) {
								go ps.unwrap(ch)
							}
							return nil, topology.ErrWantSelf
						}
						ps.logger.Debug("no peers left", "chunk_address", ch.Address(), "error", err)
						return nil, err
					}
					continue // there is still an inflight request, wait for it's result
				}

				ps.logger.Debug("sleeping to refresh overdraft balance", "chunk_address", ch.Address())

				select {
				case <-time.After(overDraftRefresh):
					retry("overDraftRefresh")
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}

			if err != nil {
				if inflight == 0 {
					return nil, err
				}
				ps.logger.Debug("next peer", "chunk_address", ch.Address(), "error", err)
				continue
			}

			// since we can reach into the neighborhood of the chunk
			// act as the multiplexer and push the chunk in parallel to multiple peers
			prox := swarm.Proximity(peer.Bytes(), ch.Address().Bytes())
			if prox >= ps.store.StorageRadius() {
				if (parallelForwards > 0) {
					ps.logger.Debug("pushTrace:pushToClosest:paralleling TRY", "chunk", ch.Address(), "peer", peer, "proximity", prox, "radius", ps.store.StorageRadius(), "parallels", parallelForwards)
				}
				for ; parallelForwards > 0; parallelForwards-- {
					nextPeer, nextErr := ps.closestPeer(ch.Address(), origin)
					if (nextErr != nil) {
						ps.logger.Debug("pushTrace:pushToClosest:paralleling err", "chunk", ch.Address(), "err", nextErr)
						break
					}
					nextProx := swarm.Proximity(nextPeer.Bytes(), ch.Address().Bytes())
					if (nextProx < ps.store.StorageRadius()) {
						ps.logger.Debug("pushTrace:pushToClosest:paralleling NOT", "chunk", ch.Address(), "nextPeer", nextPeer, "proximity", nextProx, "radius", ps.store.StorageRadius())
						break
					} else {
						retry("parallelForwards("+strconv.Itoa(parallelForwards)+")")
						sentErrorsLeft++
					}
				}
			}

			action, err := ps.prepareCredit(ctx, peer, ch, origin)
			if err != nil {
				retry("prepareCredit err")
				ps.skipList.Add(ch.Address(), peer, overDraftRefresh)
				continue
			}
			ps.skipList.Add(ch.Address(), peer, sanctionWait)

			ps.metrics.TotalSendAttempts.Inc()
			inflight++

			ps.logger.Debug("pushTrace:pushToClosest:pushing", "chunk", ch.Address(), "peer", peer)
			go ps.push(ctx, resultChan, peer, ch, action)

		case result := <-resultChan:

			inflight--

			ps.measurePushPeer(result.pushTime, result.err)

			if result.err == nil {
				ps.logger.Debug("pushTrace:pushToClosest:result", "chunk", ch.Address(), "peer", result.peer)
				return result.receipt, nil
			}

			ps.metrics.TotalFailedSendAttempts.Inc()
			ps.logger.Debug("could not push to peer", "chunk_address", ch.Address(), "peer_address", result.peer, "error", result.err)

			sentErrorsLeft--

			retry("result err")
		}
	}

	return nil, ErrNoPush
}

func (ps *PushSync) closestPeer(chunkAddress swarm.Address, origin bool) (swarm.Address, error) {

	skipList := ps.skipList.ChunkPeers(chunkAddress)
	includeSelf := ps.fullNode && !origin

	peer, err := ps.topologyDriver.ClosestPeer(chunkAddress, includeSelf, topology.Select{Reachable: true, Healthy: true}, skipList...)
	if errors.Is(err, topology.ErrNotFound) {
		peer, err := ps.topologyDriver.ClosestPeer(chunkAddress, includeSelf, topology.Select{Reachable: true}, skipList...)
		if errors.Is(err, topology.ErrNotFound) {
			return ps.topologyDriver.ClosestPeer(chunkAddress, includeSelf, topology.Select{}, skipList...)
		}
		return peer, err
	}

	return peer, err
}

func (ps *PushSync) push(parentCtx context.Context, resultChan chan<- receiptResult, peer swarm.Address, ch swarm.Chunk, action accounting.Action) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTTL)
	defer cancel()

	var (
		err     error
		receipt *pb.Receipt
	)

	now := time.Now()

	spanInner, _, _ := ps.tracer.FollowSpanFromContext(context.WithoutCancel(parentCtx), "push-chunk-async", ps.logger, opentracing.Tag{Key: "address", Value: ch.Address().String()})

	defer func() {
		if err != nil {
			ext.LogError(spanInner, err)
			ps.logger.Debug("pushTrace:push err", "chunk", ch.Address(), "peer", peer, "err", err)
		} else {
			spanInner.LogFields(olog.Bool("success", true))
			ps.logger.Debug("pushTrace:push success", "chunk", ch.Address(), "peer", peer)
		}
		spanInner.Finish()
		select {
		case resultChan <- receiptResult{pushTime: now, peer: peer, err: err, receipt: receipt}:
		case <-parentCtx.Done():
		}
	}()

	defer action.Cleanup()

	spanInner.LogFields(olog.String("peer_address", peer.String()))

	receipt, err = ps.pushChunkToPeer(tracing.WithContext(ctx, spanInner.Context()), peer, ch)
	if err != nil {
		ps.logger.Debug("pushTrace:pushChunkToPeer failed", "chunk", ch.Address(), "peer", peer, "err", err)
		return
	}

	ps.metrics.TotalSent.Inc()

	err = action.Apply()
}

func (ps *PushSync) pushChunkToPeer(ctx context.Context, peer swarm.Address, ch swarm.Chunk) (receipt *pb.Receipt, err error) {

	ps.logger.Debug("pushTrace:pushChunkToPeer", "chunk", ch.Address(), "peer", peer)
	streamer, err := ps.streamer.NewStream(ctx, peer, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		return nil, fmt.Errorf("new stream for peer %s: %w", peer.String(), err)
	}

	defer func() {
		if err != nil {
			_ = streamer.Reset()
		} else {
			_ = streamer.FullClose()
		}
	}()

	w, r := protobuf.NewWriterAndReader(streamer)
	stamp, err := ch.Stamp().MarshalBinary()
	if err != nil {
		return nil, err
	}
	err = w.WriteMsgWithContext(ctx, &pb.Delivery{
		Address: ch.Address().Bytes(),
		Data:    ch.Data(),
		Stamp:   stamp,
	})
	if err != nil {
		return nil, err
	}

	err = ps.store.Report(ctx, ch, storage.ChunkSent, ps.logger)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		err = fmt.Errorf("tag %d increment: %w", ch.TagID(), err)
		return
	}

	var rec pb.Receipt
	if err = r.ReadMsgWithContext(ctx, &rec); err != nil {
		return nil, err
	}
	if rec.Err != "" {
		return nil, p2p.NewChunkDeliveryError(rec.Err)
	}

	if !ch.Address().Equal(swarm.NewAddress(rec.Address)) {
		return nil, fmt.Errorf("invalid receipt. chunk %s, peer %s", ch.Address(), peer)
	}

	return &rec, nil
}

func (ps *PushSync) prepareCredit(ctx context.Context, peer swarm.Address, ch swarm.Chunk, origin bool) (accounting.Action, error) {
	creditAction, err := ps.accounting.PrepareCredit(ctx, peer, ps.pricer.PeerPrice(peer, ch.Address()), origin)
	if err != nil {
		return nil, err
	}

	return creditAction, nil
}

func (ps *PushSync) measurePushPeer(t time.Time, err error) {
	var status string
	if err != nil {
		status = "failure"
	} else {
		status = "success"
	}
	ps.metrics.PushToPeerTime.WithLabelValues(status).Observe(time.Since(t).Seconds())
}

func (ps *PushSync) validStampWrapper(f postage.ValidStampFn) postage.ValidStampFn {
	return func(c swarm.Chunk) (swarm.Chunk, error) {
		t := time.Now()
		chunk, err := f(c)
		if err != nil {
			ps.metrics.InvalidStampErrors.Inc()
			ps.metrics.StampValidationTime.WithLabelValues("failure").Observe(time.Since(t).Seconds())
		} else {
			ps.metrics.StampValidationTime.WithLabelValues("success").Observe(time.Since(t).Seconds())
		}
		return chunk, err
	}
}

func (s *PushSync) Close() error {
	return s.skipList.Close()
}

func (ps *PushSync) warmedUp() bool {
	return time.Now().After(ps.warmupPeriod)
}
