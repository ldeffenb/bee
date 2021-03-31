// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pullsync provides the pullsync protocol
// implementation.
package pullsync

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/ethersphere/bee/pkg/bitvector"
	"github.com/ethersphere/bee/pkg/cac"
	"github.com/ethersphere/bee/pkg/logging"
	"github.com/ethersphere/bee/pkg/p2p"
	"github.com/ethersphere/bee/pkg/p2p/protobuf"
	"github.com/ethersphere/bee/pkg/pullsync/pb"
	"github.com/ethersphere/bee/pkg/pullsync/pullstorage"
	"github.com/ethersphere/bee/pkg/soc"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

const (
	protocolName     = "pullsync"
	protocolVersion  = "1.0.0"
	streamName       = "pullsync"
	cursorStreamName = "cursors"
	cancelStreamName = "cancel"
)

var (
	ErrUnsolicitedChunk = errors.New("peer sent unsolicited chunk")

	cancellationTimeout = 5 * time.Second // explicit ruid cancellation message timeout
)

// how many maximum chunks in a batch
var maxPage = 50

// Interface is the PullSync interface.
type Interface interface {
	// SyncInterval syncs a requested interval from the given peer.
	// It returns the BinID of highest chunk that was synced from the given
	// interval. If the requested interval is too large, the downstream peer
	// has the liberty to provide less chunks than requested.
	SyncInterval(ctx context.Context, peer swarm.Address, bin uint8, from, to uint64) (topmost uint64, ruid uint32, err error)
	// GetCursors retrieves all cursors from a downstream peer.
	GetCursors(ctx context.Context, peer swarm.Address) ([]uint64, error)
	// CancelRuid cancels active pullsync operation identified by ruid on
	// a downstream peer.
	CancelRuid(ctx context.Context, peer swarm.Address, ruid uint32) error
}

type Syncer struct {
	base swarm.Address         // this node's overlay address
	streamer p2p.Streamer
	metrics  metrics
	logger   logging.Logger
	storage  pullstorage.Storer
	quit     chan struct{}
	wg       sync.WaitGroup
	unwrap   func(swarm.Chunk)

	ruidMtx sync.Mutex
	ruidCtx map[uint32]func()

	pullMtx sync.Mutex
	pullSem map[string]semChannel
	pullCount map[string]int

	syncMtx sync.Mutex
	syncSem map[string]semChannel
	//syncCount map[string]int
	
	syncIntervalThrottle semChannel
	offerThrottle semChannel
	finalThrottle semChannel

	Interface
	io.Closer
}

type semChannel chan struct {}

func New(base swarm.Address, streamer p2p.Streamer, storage pullstorage.Storer, unwrap func(swarm.Chunk), logger logging.Logger) *Syncer {
	return &Syncer{
		base:     base,
		streamer: streamer,
		storage:  storage,
		metrics:  newMetrics(),
		unwrap:   unwrap,
		logger:   logger,
		ruidCtx:  make(map[uint32]func()),
		pullCount: make(map[string]int),
		pullSem:  make(map[string]semChannel),
		syncSem:  make(map[string]semChannel),
		wg:       sync.WaitGroup{},
		quit:     make(chan struct{}),
		
		syncIntervalThrottle: make(chan struct{}, 1),
		offerThrottle: make(chan struct{}, 1),
		finalThrottle: make(chan struct{}, 1),
	}
}

func (s *Syncer) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamName,
				Handler: s.handler,
			},
			{
				Name:    cursorStreamName,
				Handler: s.cursorHandler,
			},
			{
				Name:    cancelStreamName,
				Handler: s.cancelHandler,
			},
		},
	}
}

// SyncInterval syncs a requested interval from the given peer.
// It returns the BinID of highest chunk that was synced from the given interval.
// If the requested interval is too large, the downstream peer has the liberty to
// provide less chunks than requested.
func (s *Syncer) SyncInterval(ctx context.Context, peer swarm.Address, bin uint8, from, to uint64) (topmost uint64, ruid uint32, err error) {

	s.metrics.SyncConcurrency1.Inc()
	defer s.metrics.SyncConcurrency1.Dec()

	s.syncMtx.Lock()
	semSync, ok := s.syncSem[peer.String()]
	if !ok {
		s.syncSem[peer.String()] = make(chan struct{}, 1)
		semSync, ok = s.syncSem[peer.String()]
	}
	//s.syncCount[peer.String()] = s.syncCount[peer.String()] + 1
	s.syncMtx.Unlock()
	
	
	if (to != math.MaxUint64) {	// Live syncs don't queue
		semSync <- struct{}{}
		defer func() { <-semSync }()
	}
	
	//s.syncMtx.Lock()
	//s.syncCount[peer.String()] = s.syncCount[peer.String()] - 1
	//startCount := s.syncCount[peer.String()]
	//s.syncMtx.Unlock()
	
	//lockDelta := time.Now().Sub(lockTime).String()
	s.metrics.SyncConcurrency2.Inc()
	defer s.metrics.SyncConcurrency2.Dec()
	
	stream, err := s.streamer.NewStream(ctx, peer, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		return 0, 0, fmt.Errorf("new stream: %w", err)
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	var ru pb.Ruid
	b := make([]byte, 4)
	_, err = rand.Read(b)
	if err != nil {
		return 0, 0, fmt.Errorf("crypto rand: %w", err)
	}

	ru.Ruid = binary.BigEndian.Uint32(b)

	w, r := protobuf.NewWriterAndReader(stream)

s.logger.Tracef("pullsync:SyncInterval:writeRuid Ruid:%d bin:%d %d-%d for %s", int(ru.Ruid), int(bin), from, to, peer.String())
	if err = w.WriteMsgWithContext(ctx, &ru); err != nil {
		return 0, 0, fmt.Errorf("write ruid: %w", err)
	}

s.logger.Tracef("pullsync:SyncInterval:writeRange Ruid:%d bin:%d %d-%d for %s", int(ru.Ruid), int(bin), from, to, peer.String())
	rangeMsg := &pb.GetRange{Bin: int32(bin), From: from, To: to}
	if err = w.WriteMsgWithContext(ctx, rangeMsg); err != nil {
		return 0, ru.Ruid, fmt.Errorf("write get range: %w", err)
	}

s.logger.Tracef("pullsync:SyncInterval:readOffer Ruid:%d bin:%d %d-%d for %s", int(ru.Ruid), int(bin), from, to, peer.String())
	var offer pb.Offer
	if err = r.ReadMsgWithContext(ctx, &offer); err != nil {
		return 0, ru.Ruid, fmt.Errorf("read offer: %w", err)
	}
s.logger.Tracef("pullsync:SyncInterval:Offer got %d Ruid:%d bin:%d %d-%d for %s", len(offer.Hashes)/swarm.HashSize, int(ru.Ruid), int(bin), from, to, peer.String())

	if len(offer.Hashes)%swarm.HashSize != 0 {
		return 0, ru.Ruid, fmt.Errorf("inconsistent hash length")
	}

	// empty interval (no chunks present in interval).
	// return the end of the requested range as topmost.
	if len(offer.Hashes) == 0 {
s.logger.Tracef("pullsync:SyncInterval:ZEROOffer Ruid:%d bin:%d %d-%d for %s", int(ru.Ruid), int(bin), from, to, peer.String())
		return offer.Topmost, ru.Ruid, nil
	}

	//if (to != math.MaxUint64) {	// Live syncs don't throttle
	{
		s.syncIntervalThrottle <- struct{}{}
		defer func() { <-s.syncIntervalThrottle }()
	}

	s.metrics.SyncConcurrency3.Inc()
	defer s.metrics.SyncConcurrency3.Dec()

	var (
		bvLen      = len(offer.Hashes) / swarm.HashSize
		wantChunks = make(map[string]struct{})
		ctr        = 0
	)

	bv, err := bitvector.New(bvLen)
	if err != nil {
		return 0, ru.Ruid, fmt.Errorf("new bitvector: %w", err)
	}

	myAddress := s.base
	myAddressBytes := myAddress.Bytes()
	peerBytes := peer.Bytes()
	for i := 0; i < len(offer.Hashes); i += swarm.HashSize {
		a := swarm.NewAddress(offer.Hashes[i : i+swarm.HashSize])
		if a.Equal(swarm.ZeroAddress) {
			// i'd like to have this around to see we don't see any of these in the logs
			s.logger.Errorf("syncer got a zero address hash on offer")
			return 0, ru.Ruid, fmt.Errorf("zero address on offer")
		}
		s.metrics.OfferCounter.Inc()

		myProximity := swarm.ExtendedProximity(a.Bytes(), myAddressBytes)
		peerProximity := swarm.ExtendedProximity(a.Bytes(), peerBytes)
		var needProximity uint8
		if myProximity >= peerProximity {
			needProximity = uint8(4)	// If I'm closer than him
		} else {
			needProximity = uint8(6)	// Because it's really close to me
		}
		if myProximity < needProximity {
//s.logger.Tracef("pullsync:SyncInterval:extended_proximity skipping %d < %d chunk %s me %s", int(myProximity), int(needProximity), a.String(), myAddress.String())
			continue
		}
//s.logger.Tracef("pullsync:SyncInterval:extended_proximity wanting %d >= %d chunk %s me %s", int(myProximity), int(needProximity), a.String(), myAddress.String())

		s.metrics.DbOpsCounter.Inc()
	
		have, err := s.storage.Has(ctx, a)
		if err != nil {
			return 0, ru.Ruid, fmt.Errorf("storage has: %w", err)
		}

		if !have {
			wantChunks[a.String()] = struct{}{}
			ctr++
			s.metrics.WantCounter.Inc()
			bv.Set(i / swarm.HashSize)
		}
	}

s.logger.Tracef("pullsync:SyncInterval:writeWant %d Ruid:%d bin:%d %d-%d for %s", len(wantChunks), int(ru.Ruid), int(bin), from, to, peer.String())
	wantMsg := &pb.Want{BitVector: bv.Bytes()}
	if err = w.WriteMsgWithContext(ctx, wantMsg); err != nil {
		return 0, ru.Ruid, fmt.Errorf("write want: %w", err)
	}

	// if ctr is zero, it means we don't want any chunk in the batch
	// thus, the following loop will not get executed and the method
	// returns immediately with the topmost value on the offer, which
	// will seal the interval and request the next one
	err = nil
	var chunksToPut []swarm.Chunk

if len(wantChunks) > 0 {
s.logger.Tracef("pullsync:SyncInterval:read %d Deliveries Ruid:%d bin:%d %d-%d for %s", len(wantChunks), int(ru.Ruid), int(bin), from, to, peer.String())
}
	for ; ctr > 0; ctr-- {
		var delivery pb.Delivery
		if err = r.ReadMsgWithContext(ctx, &delivery); err != nil {
			// this is not a fatal error and we should write
			// a partial batch if some chunks have been received.
			err = fmt.Errorf("read delivery: %w", err)
			break
		}

		addr := swarm.NewAddress(delivery.Address)
		if _, ok := wantChunks[addr.String()]; !ok {
			// this is fatal for the entire batch, return the
			// error and don't write the partial batch.
			return 0, ru.Ruid, ErrUnsolicitedChunk
		}

		delete(wantChunks, addr.String())
		s.metrics.DeliveryCounter.Inc()

		chunk := swarm.NewChunk(addr, delivery.Data)
		if cac.Valid(chunk) {
			go s.unwrap(chunk)
		} else if !soc.Valid(chunk) {
			// this is fatal for the entire batch, return the
			// error and don't write the partial batch.
			return 0, ru.Ruid, swarm.ErrInvalidChunk
		}
		chunksToPut = append(chunksToPut, chunk)
	}

if len(chunksToPut) > 0 {
s.logger.Tracef("pullsync:SyncInterval:put %d Chunks Ruid:%d bin:%d %d-%d for %s", len(chunksToPut), int(ru.Ruid), int(bin), from, to, peer.String())
}
	if len(chunksToPut) > 0 {
		startDB := time.Now()
		s.metrics.DbOpsCounter.Inc()
		if ierr := s.storage.Put(ctx, storage.ModePutSync, chunksToPut...); ierr != nil {
			if err != nil {
				ierr = fmt.Errorf(", sync err: %w", err)
			}
			return 0, ru.Ruid, fmt.Errorf("delivery put: %w", ierr)
		} else {
			s.logger.Tracef("pullsync:SyncInterval %s to Put %d chunks", time.Since(startDB), len(chunksToPut))
		}
	}

	// there might have been an error in the for loop above,
	// return it if it indeed happened
	if err != nil {
		return 0, ru.Ruid, err
	}

s.logger.Tracef("pullsync:SyncInterval:DONE %d/%d Chunks Ruid:%d bin:%d %d-%d for %s", len(chunksToPut), len(offer.Hashes)/swarm.HashSize, int(ru.Ruid), int(bin), from, to, peer.String())
	return offer.Topmost, ru.Ruid, nil
}

// handler handles an incoming request to sync an interval
func (s *Syncer) handler(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {

	start := time.Now()
	s.metrics.HandlerServiceCounter.Inc()
	s.metrics.HandlerConcurrency1.Inc()
	defer s.metrics.HandlerConcurrency1.Dec()

	w, r := protobuf.NewWriterAndReader(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()
	var ru pb.Ruid
	if err := r.ReadMsgWithContext(ctx, &ru); err != nil {
		return fmt.Errorf("send ruid: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	s.ruidMtx.Lock()
	s.ruidCtx[ru.Ruid] = cancel
	s.ruidMtx.Unlock()
	cc := make(chan struct{})
	defer close(cc)
	go func() {
		select {
		case <-s.quit:
		case <-ctx.Done():
		case <-cc:
		}
		cancel()
		s.ruidMtx.Lock()
		delete(s.ruidCtx, ru.Ruid)
		s.ruidMtx.Unlock()
	}()

	select {
	case <-s.quit:
		return nil
	default:
	}

	s.wg.Add(1)
	defer s.wg.Done()

	var rn pb.GetRange
	if err := r.ReadMsgWithContext(ctx, &rn); err != nil {
		return fmt.Errorf("read get range: %w", err)
	}

	lockTime := time.Now()
	s.pullMtx.Lock()
	semPull, ok := s.pullSem[p.Address.String()]
	if !ok {
		s.pullSem[p.Address.String()] = make(chan struct{}, 1)
		semPull, ok = s.pullSem[p.Address.String()]
	}
	s.pullCount[p.Address.String()] = s.pullCount[p.Address.String()] + 1
	s.pullMtx.Unlock()
	
	if (rn.To != math.MaxUint64) {	// Live syncs don't queue
		semPull <- struct{}{}
		defer func() { <-semPull }()
	}
	
	s.pullMtx.Lock()
	s.pullCount[p.Address.String()] = s.pullCount[p.Address.String()] - 1
	startCount := s.pullCount[p.Address.String()]
	s.pullMtx.Unlock()
	
	s.metrics.HandlerConcurrency2.Inc()
	defer s.metrics.HandlerConcurrency2.Dec()
	
	lockDelta := time.Now().Sub(lockTime).String()

	if (rn.To != math.MaxUint64) {	// Live syncs don't throttle
		s.offerThrottle <- struct{}{}
	}

s.logger.Tracef("SENT:pullsync-handler:makeOffer %s(w:%s) ruid:%d bin:%d %d-%d %s", time.Since(start), lockDelta, int(ru.Ruid), rn.Bin, rn.From, rn.To, p.Address.String())
	// make an offer to the upstream peer in return for the requested range
	offer, _, err := s.makeOffer(ctx, rn, p.Address)
	if err != nil {
		if (rn.To != math.MaxUint64) {	// Live syncs didn't throttle
			<-s.offerThrottle
		}
		return fmt.Errorf("make offer: %w", err)
	}

	if (rn.To != math.MaxUint64) {	// Live syncs didn't throttle
		<-s.offerThrottle
	}

	s.metrics.HandlerConcurrency3.Inc()
	defer s.metrics.HandlerConcurrency3.Dec()
	actualStart := time.Now()

s.logger.Tracef("SENT:pullsync-handler:write-offer %s(w:%s) ruid:%d bin:%d %d-%d (%d)->%d to %s", time.Since(actualStart), lockDelta, int(ru.Ruid), rn.Bin, rn.From, rn.To, len(offer.Hashes)/swarm.HashSize, offer.Topmost, p.Address.String())
	if err := w.WriteMsgWithContext(ctx, offer); err != nil {
		return fmt.Errorf("write offer: %w", err)
	}

	// we don't have any hashes to offer in this range (the
	// interval is empty). nothing more to do
	if len(offer.Hashes) == 0 {
		s.logger.Tracef("SENT:pullsync-handler:offer %s(w:%s) ruid:%d bin:%d %d-%d ZERO to %s", time.Since(actualStart), lockDelta, int(ru.Ruid), rn.Bin, rn.From, rn.To, p.Address.String())
		return nil
	}

s.logger.Tracef("SENT:pullsync-handler:readWant %s(w:%s) ruid:%d bin:%d %d-%d (%d) to %s", time.Since(actualStart), lockDelta, int(ru.Ruid), rn.Bin, rn.From, rn.To, len(offer.Hashes)/swarm.HashSize, p.Address.String())
wantStart := time.Now()

	var want pb.Want
	if err := r.ReadMsgWithContext(ctx, &want); err != nil {
		return fmt.Errorf("read want: %w", err)
	}
if time.Since(wantStart) > time.Duration(3)*time.Second {
	s.logger.Tracef("SENT:pullsync-handler:readWant LONG %s ruid:%d bin:%d %d-%d (%d) to %s", time.Since(wantStart), int(ru.Ruid), rn.Bin, rn.From, rn.To, len(offer.Hashes)/swarm.HashSize, p.Address.String())
}

	relockStart := time.Now()
	s.finalThrottle <- struct{}{}	// Block final processing to one at a time for network bandwidth smoothing
	defer func() { <-s.finalThrottle }()
	finalStart := time.Now()
	
s.logger.Tracef("SENT:pullsync-handler:processWant relock:%s %s(w:%s) ruid:%d bin:%d %d-%d ?/%d to %s", time.Since(relockStart), time.Since(actualStart), lockDelta, int(ru.Ruid), rn.Bin, rn.From, rn.To, len(offer.Hashes)/swarm.HashSize, p.Address.String())
	chs, err := s.processWant(ctx, offer, &want)
	if err != nil {
		return fmt.Errorf("process want: %w", err)
	}

s.logger.Tracef("SENT:pullsync-handler:deliver %s(w:%s) ruid:%d bin:%d %d-%d %d/%d to %s", time.Since(actualStart), lockDelta, int(ru.Ruid), rn.Bin, rn.From, rn.To, len(chs), len(offer.Hashes)/swarm.HashSize, p.Address.String())
	for _, v := range chs {
		s.metrics.HandlerDeliveryCounter.Inc()
		deliver := pb.Delivery{Address: v.Address().Bytes(), Data: v.Data()}
		if err := w.WriteMsgWithContext(ctx, &deliver); err != nil {
			return fmt.Errorf("write delivery: %w", err)
		}
	}
	s.pullMtx.Lock()
	endCount := s.pullCount[p.Address.String()]
	s.pullMtx.Unlock()

	s.logger.Tracef("SENT:pullsync-handler:delivered final:%s %s(w:%s %d->%d) ruid:%d bin:%d %d-%d %d/%d to %s", time.Since(finalStart), time.Since(actualStart), lockDelta, startCount, endCount, int(ru.Ruid), rn.Bin, rn.From, rn.To, len(chs), len(offer.Hashes)/swarm.HashSize, p.Address.String())

	time.Sleep(50 * time.Millisecond) // because of test, getting EOF w/o
	return nil
}

// makeOffer tries to assemble an offer for a given requested interval.
func (s *Syncer) makeOffer(ctx context.Context, rn pb.GetRange, peer swarm.Address) (o *pb.Offer, addrs []swarm.Address, err error) {

	o = new(pb.Offer)
	o.Hashes = make([]byte, 0)
	myAddress := s.base
	myAddressBytes := myAddress.Bytes()
	peerBytes := peer.Bytes()
	sharedBits := swarm.ExtendedProximity(myAddressBytes, peerBytes)
	
	if int(sharedBits) < 4 && int(rn.Bin) > 4 && rn.To != math.MaxUint64 {
	        s.logger.Tracef("pullsync:makeOffer:bin %d/%d %d-%d IMPOSSIBLE for %s", int(rn.Bin), int(sharedBits), rn.From, rn.To, peer.String())
		o.Topmost = rn.To	// Nothing more to ask from here
		return o, nil, nil
	}
	if rn.From == 0 && rn.To == 1 {
        s.logger.Tracef("pullsync:makeOffer:bin %d/%d %d-%d patch to avoid ZERO for %s", int(rn.Bin), int(sharedBits), rn.From, rn.To, peer.String())
		rn.To = math.MaxUint64
	}
	start := rn.From
	rejects := 0
	startTime := time.Now()
	maxDelay := time.Duration(1)*time.Minute

	for start < rn.To && rejects < maxPage * 100 {
		chs, top, err := s.storage.IntervalChunks(ctx, uint8(rn.Bin), start, rn.To, maxPage)
		if err != nil {
			return o, nil, err
		}
		o.Topmost = top
		for _, v := range chs {

			myProximity := swarm.ExtendedProximity(v.Bytes(), myAddressBytes)
			peerProximity := swarm.ExtendedProximity(v.Bytes(), peerBytes)
			var needProximity uint8
			if myProximity <= peerProximity {
				needProximity = uint8(4)	// He's closer or tied with me
			} else {
				needProximity = uint8(6)	// Because it's really close to him
			}
			if peerProximity < needProximity {
//s.logger.Tracef("pullsync:makeOffer:extended_proximity %d skipping %d < %d chunk %s peer %s", int(myProximity), int(peerProximity), int(needProximity), v.String(), peer.String())
				rejects++
				continue
			}
//s.logger.Tracef("pullsync:makeOffer:extended_proximity %d wanting %d >= %d chunk %s peer %s", int(myProximity), int(peerProximity), int(needProximity), v.String(), peer.String())
//if int(needProximity) > int(sharedBits) {
//	s.logger.Tracef("pullsync:makeOffer:OOPS:bin %d/%d/%d extended_proximity %d wanting %d >= %d chunk %s peer %s", int(rn.Bin), int(sharedBits), int(needProximity), int(myProximity), int(peerProximity), int(needProximity), v.String(), peer.String())
//}
			o.Hashes = append(o.Hashes, v.Bytes()...)
			s.metrics.HandlerOfferCounter.Inc()
		}
s.logger.Tracef("pullsync:makeOffer:bin %d/%d %d-%d got (%d)->%d for %s", int(rn.Bin), int(sharedBits), start, rn.To, len(o.Hashes)/swarm.HashSize, top, peer.String())
//if len(o.Hashes)/swarm.HashSize > 0 && int(rn.Bin) > int(sharedBits) && int(rn.Bin) < 4 {
//	s.logger.Tracef("pullsync:makeOffer:OOPS:bin %d/%d %d-%d got (%d)->%d for %s", int(rn.Bin), int(sharedBits), start, rn.To, len(o.Hashes)/swarm.HashSize, top, peer.String())
//}
		if len(o.Hashes)/swarm.HashSize > 0 || time.Since(startTime) >= maxDelay  {
if int(sharedBits) < 4 && int(rn.Bin) > 4 && len(o.Hashes)/swarm.HashSize > 0 {
	s.logger.Tracef("pullsync:makeOffer:OOPS:bin %d/%d %d-%d got (%d)->%d for %s", int(rn.Bin), int(sharedBits), start, rn.To, len(o.Hashes)/swarm.HashSize, top, peer.String())
}
			
			return o, chs, nil
		}
		start = top + 1		// just in case we need more
	}
	return o, nil, nil
	//return o, chs, nil
}

// processWant compares a received Want to a sent Offer and returns
// the appropriate chunks from the local store.
func (s *Syncer) processWant(ctx context.Context, o *pb.Offer, w *pb.Want) ([]swarm.Chunk, error) {
	l := len(o.Hashes) / swarm.HashSize
	bv, err := bitvector.NewFromBytes(w.BitVector, l)
	if err != nil {
		return nil, err
	}

	var addrs []swarm.Address
	for i := 0; i < len(o.Hashes); i += swarm.HashSize {
		if bv.Get(i / swarm.HashSize) {
			a := swarm.NewAddress(o.Hashes[i : i+swarm.HashSize])
			addrs = append(addrs, a)
			s.metrics.HandlerWantCounter.Inc()
		}
	}
	s.metrics.HandlerDbOpsCounter.Inc()
	return s.storage.Get(ctx, storage.ModeGetSync, addrs...)
}

func (s *Syncer) GetCursors(ctx context.Context, peer swarm.Address) (retr []uint64, err error) {
	stream, err := s.streamer.NewStream(ctx, peer, nil, protocolName, protocolVersion, cursorStreamName)
	if err != nil {
		return nil, fmt.Errorf("new stream: %w", err)
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	w, r := protobuf.NewWriterAndReader(stream)
	syn := &pb.Syn{}
	if err = w.WriteMsgWithContext(ctx, syn); err != nil {
		return nil, fmt.Errorf("write syn: %w", err)
	}

	var ack pb.Ack
	if err = r.ReadMsgWithContext(ctx, &ack); err != nil {
		return nil, fmt.Errorf("read ack: %w", err)
	}

	retr = ack.Cursors

	return retr, nil
}

func (s *Syncer) cursorHandler(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	w, r := protobuf.NewWriterAndReader(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()

	var syn pb.Syn
	if err := r.ReadMsgWithContext(ctx, &syn); err != nil {
		return fmt.Errorf("read syn: %w", err)
	}

	var ack pb.Ack
	s.metrics.CursorDbOpsCounter.Inc()
	ints, err := s.storage.Cursors(ctx)
	if err != nil {
		return err
	}
	ack.Cursors = ints
	if err = w.WriteMsgWithContext(ctx, &ack); err != nil {
		return fmt.Errorf("write ack: %w", err)
	}

	return nil
}

func (s *Syncer) CancelRuid(ctx context.Context, peer swarm.Address, ruid uint32) (err error) {
	stream, err := s.streamer.NewStream(ctx, peer, nil, protocolName, protocolVersion, cancelStreamName)
	if err != nil {
		return fmt.Errorf("new stream: %w", err)
	}

	w := protobuf.NewWriter(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, cancellationTimeout)
	defer cancel()

	var c pb.Cancel
	c.Ruid = ruid
	if err := w.WriteMsgWithContext(ctx, &c); err != nil {
		return fmt.Errorf("send cancellation: %w", err)
	}
	return nil
}

// handler handles an incoming request to explicitly cancel a ruid
func (s *Syncer) cancelHandler(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	r := protobuf.NewReader(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()

	var c pb.Cancel
	if err := r.ReadMsgWithContext(ctx, &c); err != nil {
		return fmt.Errorf("read cancel: %w", err)
	}

	s.ruidMtx.Lock()
	defer s.ruidMtx.Unlock()

	if cancel, ok := s.ruidCtx[c.Ruid]; ok {
		cancel()
	}
	delete(s.ruidCtx, c.Ruid)
	return nil
}

func (s *Syncer) Close() error {
	s.logger.Info("pull syncer shutting down")
	close(s.quit)
	cc := make(chan struct{})
	go func() {
		defer close(cc)
		s.wg.Wait()
	}()
	select {
	case <-cc:
	case <-time.After(10 * time.Second):
		s.logger.Warning("pull syncer shutting down with running goroutines")
	}
	return nil
}
