// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package batchstore implements the reserve
// the reserve serves to maintain chunks in the area of responsibility
// it has two components
// -  the batchstore reserve which maintains information about batches, their values, priorities and synchronises with the blockchain
// - the localstore which stores chunks and manages garbage collection
//
// when a new chunk arrives in the localstore, the batchstore reserve is asked to check
// the batch used in the postage stamp attached to the chunk.
// Depending on the value of the batch (reserve depth of the batch), the localstore
// either pins the chunk (thereby protecting it from garbage collection) or not.
// the chunk stays pinned until it is 'unreserved' based on changes in relative priority of the batch it belongs to
//
// the atomic db operation is unreserving a batch down to a depth
// the intended semantics of unreserve is to unpin the chunks
// in the relevant POs, belonging to the batch and (unless they are otherwise pinned)
// allow  them  to be gargage collected.
//
// the rules of the reserve
// - if batch a is unreserved and val(b) <  val(a) then b is unreserved on any po
// - if a batch is unreserved on po  p, then  it is unreserved also on any p'<p
// - batch size based on fully filled the reserve should not  exceed Capacity
// - batch reserve is maximally utilised, i.e, cannot be extended and have 1-3 remain true
package batchstore

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/swarm"
)

// ErrBatchNotFound is returned when the postage batch is not found or expired
var ErrBatchNotFound = errors.New("postage batch not found or expired")

// DefaultDepth is the initial depth for the reserve
const DefaultDepth = 5

// Capacity = number of chunks in reserve. `2^23` was chosen to remain
// relatively near the current 5M chunks ~25GB
var Capacity = exp2(23)

var big1 = big.NewInt(1)

// reserveState records the state and is persisted in the state store
type reserveState struct {
	Depth    uint8    `json:"depth"`    // Radius of responsibility
	Capacity int64    `json:"capacity"` // size of the reserve -  number of chunks
	Edge     *big.Int `json:"edge"`     // lower value limit for edge layer = the further half of chunks
	Core     *big.Int `json:"core"`     // lower value limit for core layer = the closer half of chunks
}

// unreserve is called when the batchstore decides not to reserve a batch on a PO
// i.e.  chunk of  the batch in bins [0 upto PO] (closed  interval) are unreserved
func (s *store) unreserve(batchID []byte, radius uint8) error {
	return s.unreserveFunc(batchID, radius)
}

// purgeExpired is called when there is 'settlement'
func (s *store) purgeExpired() error {
	var toDelete [][]byte
	err := s.store.Iterate(valueKeyPrefix, func(key, _ []byte) (bool, error) {
		b, err := s.Get(valueKeyToID(key))
		if err != nil {
			return true, err
		}
		if b.Value.Cmp(s.rs.Core) >= 0 {
			return true, nil
		}
		err = s.unreserve(b.ID, swarm.MaxPO)
		if err != nil {
			return true, err
		}
		// if batch has no value then delete it
		if b.Value.Cmp(s.cs.Total) <= 0 {
			toDelete = append(toDelete, b.ID)
		} else {
			b.Radius = swarm.MaxPO
			if err := s.store.Put(batchKey(b.ID), b); err != nil {
				return true, err
			}
		}

		return false, nil
	})
	if err != nil {
		return err
	}
	return s.delete(toDelete...)
}

// change captures the batch's change in value
type change struct {
	old, new int
}

func newChange(oldv, newv, lower, higher *big.Int) *change {
	return &change{
		old: cmp(oldv, lower, higher),
		new: cmp(newv, lower, higher),
	}
}

// cmp checks where value x of a batch falls wrt core and edge layer limit
// it returns
// - -1 if x is out of the reserve
// - 0 if x is in the mid range where chunks are kept within depth
// - 1 if x is in the top range where chunks are kept within depth - 1
func cmp(x, lower, higher *big.Int) int {
	if x.Cmp(lower) < 0 || x.Cmp(big.NewInt(0)) == 0 {
		return -1
	}
	if x.Cmp(higher) < 0 {
		return 0
	}
	return 1
}

// if value changed wrt range
func (ch *change) changed() bool {
	return ch.old != ch.new
}

// if the value is increased
func (ch *change) increased() bool {
	return ch.old < ch.new
}

// if value changed from -1 to 1 or 1 to -1
func (ch *change) double() bool {
	return ch.old+ch.new == 0
}

// if currently out of reserve, ie., new value falls in bottom range
func (ch *change) out() bool {
	return ch.new < 0
}

// updateReserveState manages what chunks of which batch are allocated to the reserve
func (s *store) updateValueChange(b *postage.Batch, oldDepth uint8, oldValue *big.Int) error {
	newValue := b.Value
	newDepth := b.Depth

	ch := newChange(oldValue, newValue, s.rs.Core, s.rs.Edge)
	half := exp2(newDepth - s.rs.Depth - 1)
	// if value drops out then available capacity is increased with the batch's size within neighbourhood
	if ch.out() {
		if ch.changed() {
			s.rs.Capacity += half
			if ch.double() {
				s.rs.Capacity += half
			}
		}
		return s.unreserve(b.ID, swarm.MaxPO)
	}
	// if value decreased and fell below the edge limit, then the freed capacity is added
	if !ch.increased() {
		s.rs.Capacity += half
		// the chunks of this batch are unreserved in PO depth
		return s.unreserve(b.ID, s.rs.Depth)
	}
	// otherwise value increased and we need to make capacity for its size
	s.rs.Capacity -= half
	// double means it got from no reserve to reserve upto full depth
	if ch.double() {
		s.rs.Capacity -= half
	}
	radius := s.rs.Depth - uint8(ch.new)
	if err := s.unreserve(b.ID, radius); err != nil {
		return err
	}
	return s.evict(b)
}

// evict is responsible for keeping capacity positive by unreserving lowest priority batches
func (s *store) evict(last *postage.Batch) error {
	// if capacity is positive nothing to evict
	if s.rs.Capacity > 0 {
		// we readjust the value  limit, if new item  is the minimum so far, reset Core limit
		if s.rs.Core.Cmp(big.NewInt(0)) == 0 || s.rs.Core.Cmp(last.Value) > 0 {
			s.rs.Core.Set(last.Value)
			s.rs.Edge.Set(last.Value)
		}
		return nil
	}
	err := s.store.Iterate(valueKeyPrefix, func(key, _ []byte) (bool, error) {
		batchID := valueKeyToID(key)
		b := last
		if !bytes.Equal(b.ID, batchID) {
			var err error
			b, err = s.Get(batchID)
			if err != nil {
				return true, fmt.Errorf("release get %x %v: %w", batchID, b, err)
			}
		}
		//  FIXME: this is needed only because  the statestore iterator does not allow seek, only prefix
		//  so we  need  to  page through all  the batches until edge limit   is reached
		if b.Value.Cmp(s.rs.Edge) < 0 {
			return false, nil
		}
		// stop iteration  only  if  we consumed all batches of the same value as the one that put capacity above zero
		if s.rs.Capacity >= 0 && s.rs.Edge.Cmp(b.Value) != 0 {
			return true, nil
		}
		// unreserve edge PO of the lowest priority batch  until capacity is back to positive
		s.rs.Capacity += exp2(b.Depth - s.rs.Depth - 1)
		s.rs.Edge.Set(b.Value)
		return false, s.unreserve(b.ID, s.rs.Depth)
	})
	if err != nil {
		return err
	}
	// add 1 to edge limit value so we dont hit on the same batch next time we iterate
	s.rs.Edge.Add(s.rs.Edge, big1)
	// if we consumed all batches, ie. we unreserved all chunks on the edge = depth PO
	//  then its time to  increase depth
	if s.rs.Capacity < 0 {
		s.rs.Depth++
		s.rs.Edge.Set(s.rs.Core) // reset edge limit to core limit
		return s.evict(last)
	}
	return s.store.Put(reserveStateKey, s.rs)
}

// exp2 returns the e-th power of 2
func exp2(e uint8) int64 {
	if e == 0 {
		return 1
	}
	b := int64(2)
	for i := uint8(1); i < e; i++ {
		b *= 2
	}
	return b
}
