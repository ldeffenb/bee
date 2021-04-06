// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package localstore

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/ethersphere/bee/pkg/shed"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/syndtr/goleveldb/leveldb"
)

// Put stores Chunks to database and depending
// on the Putter mode, it updates required indexes.
// Put is required to implement storage.Store
// interface.
func (db *DB) Put(ctx context.Context, mode storage.ModePut, chs ...swarm.Chunk) (exist []bool, err error) {

	db.metrics.ModePut.Inc()
	defer totalTimeMetric(db.metrics.TotalTimePut, time.Now())

	exist, err = db.put(mode, chs...)
	if err != nil {
		db.metrics.ModePutFailure.Inc()
	}

	return exist, err
}

// put stores Chunks to database and updates other indexes. It acquires lockAddr
// to protect two calls of this function for the same address in parallel. Item
// fields Address and Data must not be with their nil values. If chunks with the
// same address are passed in arguments, only the first chunk will be stored,
// and following ones will have exist set to true for their index in exist
// slice. This is the same behaviour as if the same chunks are passed one by one
// in multiple put method calls.
func (db *DB) put(mode storage.ModePut, chs ...swarm.Chunk) (exist []bool, err error) {
	// protect parallel updates
	startLock := time.Now()
	db.metrics.BatchLockHitPut.Inc()
	db.batchMu.Lock()
	totalTimeMetric(db.metrics.BatchLockWaitTimePut, startLock)
	defer func(waitTime time.Duration, start time.Time) {
		if len(chs) == 1 {
			db.logger.Debugf("put(%s) %d chunks waited %s executed %s %s", mode.String(), len(chs), waitTime, time.Since(start), chs[0].Address().String())
		} else {
			db.logger.Debugf("put(%s) %d chunks waited %s executed %s", mode.String(), len(chs), waitTime, time.Since(start))
		}
	}(time.Since(startLock), time.Now())
	defer totalTimeMetric(db.metrics.BatchLockHeldTimePut, time.Now())
	defer db.batchMu.Unlock()
	lockTime := time.Since(startLock)
	if db.gcRunning {
		for _, ch := range chs {
			db.dirtyAddresses = append(db.dirtyAddresses, ch.Address())
		}
	}

	batch := new(leveldb.Batch)

	// variables that provide information for operations
	// to be done after write batch function successfully executes
	var gcSizeChange int64                      // number to add or subtract from gcSize
	var triggerPushFeed bool                    // signal push feed subscriptions to iterate
	triggerPullFeed := make(map[uint8]struct{}) // signal pull feed subscriptions to iterate

	exist = make([]bool, len(chs))

	// A lazy populated map of bin ids to properly set
	// BinID values for new chunks based on initial value from database
	// and incrementing them.
	// Values from this map are stored with the batch
	binIDs := make(map[uint8]uint64)

	switch mode {
	case storage.ModePutRequest, storage.ModePutRequestPin:
		for i, ch := range chs {
			if containsChunk(ch.Address(), chs[:i]...) {
				exist[i] = true
				continue
			}
			exists, c, err := db.putRequest(batch, binIDs, chunkToItem(ch))
			if err != nil {
				return nil, err
			}
			exist[i] = exists
			gcSizeChange += c

			if mode == storage.ModePutRequestPin {
				err = db.setPin(batch, ch.Address())
				if err != nil {
					return nil, err
				}
			}
		}

	case storage.ModePutUpload, storage.ModePutUploadPin:
		for i, ch := range chs {
			if containsChunk(ch.Address(), chs[:i]...) {
				db.logger.Debugf("put(%s) DUPLICATE %s", mode.String(), ch.Address().String())
				exist[i] = true
				continue
			}
			exists, c, err := db.putUpload(batch, binIDs, chunkToItem(ch))
			if err != nil {
				return nil, err
			}
			exist[i] = exists
			if !exists {
				// chunk is new so, trigger subscription feeds
				// after the batch is successfully written
				triggerPullFeed[db.po(ch.Address())] = struct{}{}
				triggerPushFeed = true
			} else {
				db.logger.Debugf("put(%s) REDUNDANT %s", mode.String(), ch.Address().String())
			}
			gcSizeChange += c
			if mode == storage.ModePutUploadPin {
				err = db.setPin(batch, ch.Address())
				if err != nil {
					return nil, err
				}
			}
		}

	case storage.ModePutForward:
		for i, ch := range chs {
			if containsChunk(ch.Address(), chs[:i]...) {
				db.logger.Debugf("put(%s) DUPLICATE %s", mode.String(), ch.Address().String())
				exist[i] = true
				continue
			}
			exists, c, err := db.putForward(batch, binIDs, chunkToItem(ch), lockTime)
			if err != nil {
				return nil, err
			}
			exist[i] = exists
			if !exists {
				// chunk is new so, trigger push subscription feed
				// after the batch is successfully written
				triggerPushFeed = true
			} else {
				db.logger.Debugf("put(%s) REDUNDANT %s", mode.String(), ch.Address().String())
			}
			gcSizeChange += c
		}

	case storage.ModePutSync:
		for i, ch := range chs {
			if containsChunk(ch.Address(), chs[:i]...) {
				db.logger.Debugf("put(%s) DUPLICATE %s", mode.String(), ch.Address().String())
				exist[i] = true
				continue
			}
			exists, c, err := db.putSync(batch, binIDs, chunkToItem(ch))
			if err != nil {
				return nil, err
			}
			exist[i] = exists
			if !exists {
				// chunk is new so, trigger pull subscription feed
				// after the batch is successfully written
				triggerPullFeed[db.po(ch.Address())] = struct{}{}
			} else {
				db.logger.Debugf("put(%s) REDUNDANT %s", mode.String(), ch.Address().String())
			}
			gcSizeChange += c
		}

	default:
		return nil, ErrInvalidMode
	}

	for po, id := range binIDs {
		db.binIDs.PutInBatch(batch, uint64(po), id)
	}

	err = db.incGCSizeInBatch(batch, gcSizeChange)
	if err != nil {
		return nil, err
	}

	err = db.shed.WriteBatch(batch)
	if err != nil {
		return nil, err
	}

	for po := range triggerPullFeed {
		db.triggerPullSubscriptions(po)
	}
	if triggerPushFeed {
		db.triggerPushSubscriptions()
	}
	return exist, nil
}

// putRequest adds an Item to the batch by updating required indexes:
//  - put to indexes: retrieve, gc
//  - it does not enter the syncpool
// The batch can be written to the database.
// Provided batch and binID map are updated.
func (db *DB) putRequest(batch *leveldb.Batch, binIDs map[uint8]uint64, item shed.Item) (exists bool, gcSizeChange int64, err error) {
	has, err := db.retrievalDataIndex.Has(item)
	if err != nil {
		return false, 0, err
	}
	if has {
		return true, 0, nil
	}

	item.StoreTimestamp = now()
	item.BinID, err = db.incBinID(binIDs, db.po(swarm.NewAddress(item.Address)))
	if err != nil {
		return false, 0, err
	}

	gcSizeChange, err = db.setGC(batch, item)
	if err != nil {
		return false, 0, err
	}

	db.metrics.TotalRequestRetrievalIndex.Inc()
	err = db.retrievalDataIndex.PutInBatch(batch, item)
	if err != nil {
		return false, 0, err
	}

	return false, gcSizeChange, nil
}

// putUpload adds an Item to the batch by updating required indexes:
//  - put to indexes: retrieve, push, pull
// The batch can be written to the database.
// Provided batch and binID map are updated.
func (db *DB) putUpload(batch *leveldb.Batch, binIDs map[uint8]uint64, item shed.Item) (exists bool, gcSizeChange int64, err error) {
	exists, err = db.retrievalDataIndex.Has(item)
	if err != nil {
		return false, 0, err
	}
	if exists {
		return true, 0, nil
	}

	item.StoreTimestamp = now()
	item.BinID, err = db.incBinID(binIDs, db.po(swarm.NewAddress(item.Address)))
	if err != nil {
		return false, 0, err
	}
	db.metrics.TotalUploadRetrievalIndex.Inc()
	err = db.retrievalDataIndex.PutInBatch(batch, item)
	if err != nil {
		return false, 0, err
	}
	err = db.pullIndex.PutInBatch(batch, item)
	if err != nil {
		return false, 0, err
	}
	err = db.pushIndex.PutInBatch(batch, item)
	if err != nil {
		return false, 0, err
	}

	return false, 0, nil
}

// putSync adds an Item to the batch by updating required indexes:
//  - put to indexes: retrieve, pull, gc
// The batch can be written to the database.
// Provided batch and binID map are updated.
func (db *DB) putSync(batch *leveldb.Batch, binIDs map[uint8]uint64, item shed.Item) (exists bool, gcSizeChange int64, err error) {

	start := time.Now()

	start = time.Now()
	db.metrics.TotalSyncRetrieve.Inc()
	exists, err = db.retrievalDataIndex.Has(item)
	totalTimeMetric(db.metrics.TotalTimeSyncRetrieve, start)
	if err != nil {
		return false, 0, err
	}
	if exists {
		return true, 0, nil
	}

	item.StoreTimestamp = now()
	start = time.Now()
	db.metrics.TotalSyncBinID.Inc()
	item.BinID, err = db.incBinID(binIDs, db.po(swarm.NewAddress(item.Address)))
	totalTimeMetric(db.metrics.TotalTimeSyncBinID, start)
	if err != nil {
		return false, 0, err
	}

	start = time.Now()
	db.metrics.TotalSyncRetrievalIndex.Inc()
	err = db.retrievalDataIndex.PutInBatch(batch, item)
	totalTimeMetric(db.metrics.TotalTimeSyncRetrievalIndex, start)
	if err != nil {
		return false, 0, err
	}

	start = time.Now()
	db.metrics.TotalSyncPullIndex.Inc()
	err = db.pullIndex.PutInBatch(batch, item)
	totalTimeMetric(db.metrics.TotalTimeSyncPullIndex, start)
	if err != nil {
		return false, 0, err
	}

	start = time.Now()
	db.metrics.TotalSyncSetGC.Inc()
	gcSizeChange, err = db.setGC(batch, item)
	totalTimeMetric(db.metrics.TotalTimeSyncSetGC, start)
	if err != nil {
		return false, 0, err
	}

	return false, gcSizeChange, nil
}

// putSync adds an Item to the batch by updating required indexes:
//  - put to indexes: retrieve, pull, push
// The batch can be written to the database.
// Provided batch and binID map are updated.
func (db *DB) putForward(batch *leveldb.Batch, binIDs map[uint8]uint64, item shed.Item, lockTime time.Duration) (exists bool, gcSizeChange int64, err error) {

	start := time.Now()

	start = time.Now()
	db.metrics.TotalForwardRetrieve.Inc()
	exists, err = db.retrievalDataIndex.Has(item)
	totalTimeMetric(db.metrics.TotalTimeForwardRetrieve, start)
	if err != nil {
		return false, 0, err
	}
	if exists {	// Even if it exists, ensure that it is in the pushIndex
		db.metrics.TotalForwardExists.Inc()
		i, err := db.retrievalDataIndex.Get(item)
		if err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
				db.logger.Warningf("put(Forward) UNKNOWN %s", swarm.NewAddress(item.Address).String())
				return false, 0, nil
			}
			return false, 0, err
		}
		item.StoreTimestamp = i.StoreTimestamp
		item.BinID = i.BinID
		_, err = db.pushIndex.Get(item)
		if err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
				// we handle this error internally, since this is an internal inconsistency of the indices
				// this error can happen if the chunk has already been pushed
				// but this function is called with ModePutForward, so we'll forward it again if it has been long enough
				var age int64
				if item.AccessTimestamp != 0 {
					age = (now()-item.AccessTimestamp)/1000000000	// Convert to seconds
				} else {
					age = (now()-item.StoreTimestamp)/1000000000	// Convert to seconds
				}
				if age > int64(math.Max(60.0,lockTime.Seconds()*2.0)) || age < 0 {	// Arbitrary age numbers
					db.logger.Tracef("localstore:putForward: chunk with address %s stored %d not found in push index, re-inserting age %ds", swarm.NewAddress(item.Address).String(), item.StoreTimestamp, age)
					start = time.Now()
					db.metrics.TotalForwardRePush.Inc()
					db.metrics.TotalForwardPushIndex.Inc()
					err = db.pushIndex.PutInBatch(batch, item)
					totalTimeMetric(db.metrics.TotalTimeForwardPushIndex, start)
					if err != nil {
						return false, 0, err
					}
					err = db.gcIndex.DeleteInBatch(batch, item)
					if err != nil {
						db.logger.Tracef("localstore:putForward: chunk with address %s stored %d not found in gc index, age %ds", swarm.NewAddress(item.Address).String(), item.StoreTimestamp, age)
					}
				} else {
					db.metrics.TotalForwardSoonPush.Inc()
					db.logger.Tracef("localstore:putForward: chunk with address %s stored %d not re-inserting age %ds", swarm.NewAddress(item.Address).String(), item.StoreTimestamp, age)
				}
			} else {
				return false, 0, err
			}
		} else {
			db.metrics.TotalForwardPendPush.Inc()
			db.logger.Tracef("localstore:putForward: chunk with address %s stored %d still in push index", swarm.NewAddress(item.Address).String(), item.StoreTimestamp)
		}
		return true, 0, nil
	}

	item.StoreTimestamp = now()

	start = time.Now()
	db.metrics.TotalForwardBinID.Inc()
	item.BinID, err = db.incBinID(binIDs, db.po(swarm.NewAddress(item.Address)))
	totalTimeMetric(db.metrics.TotalTimeForwardBinID, start)
	if err != nil {
		return false, 0, err
	}

	start = time.Now()
	db.metrics.TotalForwardRetrievalIndex.Inc()
	err = db.retrievalDataIndex.PutInBatch(batch, item)
	totalTimeMetric(db.metrics.TotalTimeForwardRetrievalIndex, start)
	if err != nil {
		return false, 0, err
	}

	start = time.Now()
	db.metrics.TotalForwardPullIndex.Inc()
	err = db.pullIndex.PutInBatch(batch, item)
	totalTimeMetric(db.metrics.TotalTimeForwardPullIndex, start)
	if err != nil {
		return false, 0, err
	}

	start = time.Now()
	db.logger.Tracef("localstore:putForward: chunk with address %s stored %d added to push index", swarm.NewAddress(item.Address).String(), item.StoreTimestamp)
	db.metrics.TotalForwardPushIndex.Inc()
	err = db.pushIndex.PutInBatch(batch, item)
	totalTimeMetric(db.metrics.TotalTimeForwardPushIndex, start)
	if err != nil {
		return false, 0, err
	}

	return false, gcSizeChange, nil
}

// setGC is a helper function used to add chunks to the retrieval access
// index and the gc index in the cases that the putToGCCheck condition
// warrants a gc set. this is to mitigate index leakage in edge cases where
// a chunk is added to a node's localstore and given that the chunk is
// already within that node's NN (thus, it can be added to the gc index
// safely)
func (db *DB) setGC(batch *leveldb.Batch, item shed.Item) (gcSizeChange int64, err error) {
	if item.BinID == 0 {
		i, err := db.retrievalDataIndex.Get(item)
		if err != nil {
			return 0, err
		}
		item.BinID = i.BinID
	}
	i, err := db.retrievalAccessIndex.Get(item)
	switch {
	case err == nil:
		item.AccessTimestamp = i.AccessTimestamp
		err = db.gcIndex.DeleteInBatch(batch, item)
		if err != nil {
			return 0, err
		}
		gcSizeChange--
	case errors.Is(err, leveldb.ErrNotFound):
		// the chunk is not accessed before
	default:
		return 0, err
	}
	item.AccessTimestamp = now()
	err = db.retrievalAccessIndex.PutInBatch(batch, item)
	if err != nil {
		return 0, err
	}

	// add new entry to gc index ONLY if it is not present in pinIndex
	ok, err := db.pinIndex.Has(item)
	if err != nil {
		return 0, err
	}
	if !ok {
		err = db.gcIndex.PutInBatch(batch, item)
		if err != nil {
			return 0, err
		}
		gcSizeChange++
	}

	return gcSizeChange, nil
}

// incBinID is a helper function for db.put* methods that increments bin id
// based on the current value in the database. This function must be called under
// a db.batchMu lock. Provided binID map is updated.
func (db *DB) incBinID(binIDs map[uint8]uint64, po uint8) (id uint64, err error) {
	if _, ok := binIDs[po]; !ok {
		binIDs[po], err = db.binIDs.Get(uint64(po))
		if err != nil {
			return 0, err
		}
	}
	binIDs[po]++
	return binIDs[po], nil
}

// containsChunk returns true if the chunk with a specific address
// is present in the provided chunk slice.
func containsChunk(addr swarm.Address, chs ...swarm.Chunk) bool {
	for _, c := range chs {
		if addr.Equal(c.Address()) {
			return true
		}
	}
	return false
}
