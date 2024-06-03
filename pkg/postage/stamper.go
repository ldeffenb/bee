// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package postage

import (
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

var (
	// ErrBucketFull is the error when a collision bucket is full.
	ErrBucketFull = errors.New("bucket full")
)

// Stamper can issue stamps from the given address of chunk.
type Stamper interface {
	Stamp(swarm.Address) (*Stamp, error)
}

// stamper connects a stampissuer with a signer.
// A stamper is created for each upload session.
type stamper struct {
	store  storage.Store
	issuer *StampIssuer
	signer crypto.Signer
	logger log.Logger
}

// NewStamper constructs a Stamper.
func NewStamper(store storage.Store, issuer *StampIssuer, signer crypto.Signer, logger log.Logger) Stamper {
	return &stamper{store, issuer, signer, logger}
}

// Stamp takes chunk, see if the chunk can be included in the batch and
// signs it with the owner of the batch of this Stamp issuer.
func (st *stamper) Stamp(addr swarm.Address) (*Stamp, error) {
	st.issuer.mtx.Lock()
	defer st.issuer.mtx.Unlock()

	tfmt := "2006-01-02T15:04:05"
	
	item := &StampItem{
		BatchID:      st.issuer.data.BatchID,
		chunkAddress: addr,
	}
	switch err := st.store.Get(item); {
	case err == nil:
		bucket, index := BucketIndexFromBytes(item.BatchIndex)
		original := item.BatchTimestamp
		item.BatchTimestamp = unixTime()
		st.logger.Debug("stampTrace: update time", "addr", addr, "batch", hex.EncodeToString(item.BatchID), "bucket", bucket, "index", index,
						"from", time.Unix(0,int64(TimestampFromBytes(original))).Format(tfmt), "to", time.Unix(0,int64(TimestampFromBytes(item.BatchTimestamp))).Format(tfmt))
		if err = st.store.Put(item); err != nil {
			st.logger.Error(err, "stampTrace: update time put err", "addr", addr, "batch", hex.EncodeToString(item.BatchID), "bucket", bucket, "index", index)
			return nil, err
		}
	case errors.Is(err, storage.ErrNotFound):
		item.BatchIndex, item.BatchTimestamp, err = st.issuer.increment(addr)
		bucket, index := BucketIndexFromBytes(item.BatchIndex)
		st.logger.Debug("stampTrace: new stamp", "addr", addr, "batch", hex.EncodeToString(item.BatchID), "bucket", bucket, "index", index,
						"time", time.Unix(0,int64(TimestampFromBytes(item.BatchTimestamp))).Format(tfmt))
		if err != nil {
			st.logger.Error(err, "stampTrace: increment err", "addr", addr, "batch", hex.EncodeToString(item.BatchID), "bucket", bucket, "index", index)
			return nil, err
		}
		if err := st.store.Put(item); err != nil {
			st.logger.Error(err, "stampTrace: put err", "addr", addr, "batch", hex.EncodeToString(item.BatchID), "bucket", bucket, "index", index)
			return nil, err
		}
	default:
		st.logger.Error(err, "stampTrace: get err", "addr", addr, "batch", hex.EncodeToString(item.BatchID))
		return nil, fmt.Errorf("get stamp for %s: %w", item, err)
	}

	toSign, err := ToSignDigest(
		addr.Bytes(),
		st.issuer.data.BatchID,
		item.BatchIndex,
		item.BatchTimestamp,
	)
	if err != nil {
		return nil, err
	}
	sig, err := st.signer.Sign(toSign)
	if err != nil {
		return nil, err
	}
	return NewStamp(st.issuer.data.BatchID, item.BatchIndex, item.BatchTimestamp, sig), nil
}
