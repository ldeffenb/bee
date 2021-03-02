// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package batchstore_test

import (
	"fmt"
	"io/ioutil"
	"math/big"
	"math/rand"
	"os"
	"testing"

	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/postage/batchstore"
	postagetest "github.com/ethersphere/bee/pkg/postage/testing"
	"github.com/ethersphere/bee/pkg/statestore/leveldb"
	"github.com/ethersphere/bee/pkg/swarm"
)

// TestBatchStoreUnreserve is testing the correct behaviour of the reserve.
// the following assumptions are tested on each modification of the batches (top up, depth increase, price change)
// - reserve exceeds capacity
// - value-consistency of unreserved POs
func TestBatchStoreUnreserve(t *testing.T) {
	// we cannot  use the mock statestore here since the iterator is not giving the right order
	// must use the leveldb statestore
	dir, err := ioutil.TempDir("", "batchstore_test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	})

	stateStore, err := leveldb.NewStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stateStore.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// temporarily reset reserve Capacity
	defer func(i int64) {
		batchstore.Capacity = i
	}(batchstore.Capacity)
	batchstore.Capacity = batchstore.Exp2(16)
	// set mock unreserve call
	unreserved := make(map[string]uint8)
	unreserveFunc := func(batchID []byte, radius uint8) error {
		unreserved[string(batchID)] = radius
		return nil
	}
	bStore, _ := batchstore.New(stateStore, unreserveFunc)

	// initilise chainstate
	err = bStore.PutChainState(&postage.ChainState{
		Block: 666,
		Total: big.NewInt(0),
		Price: big.NewInt(100),
	})
	if err != nil {
		t.Fatal(err)
	}

	// iterate starting from batchstore.DefaultDepth to maxPO
	_, depth := batchstore.GetReserve(bStore)
	for step := 0; depth < swarm.MaxPO; step++ {
		cs, err := nextChainState(bStore)
		if err != nil {
			t.Fatal(err)
		}
		if err = addBatch(bStore, cs, depth); err != nil {
			t.Fatal(err)
		}
		if depth, err = checkReserve(bStore, unreserved); err != nil {
			t.Fatal(err)
		}
	}
}

func nextChainState(bStore postage.Storer) (*postage.ChainState, error) {
	cs := bStore.GetChainState()
	// random advance on the blockchain
	advance := rand.Intn(10) + 1
	cs = &postage.ChainState{
		Block: cs.Block + uint64(advance),
		Price: cs.Price,
		// settle although no price change
		Total: cs.Total.Add(cs.Total, big.NewInt(0).Mul(cs.Price, big.NewInt(int64(advance)))),
	}
	return cs, bStore.PutChainState(cs)
}

func addBatch(bStore postage.Storer, cs *postage.ChainState, depth uint8) error {
	// create random batch
	b := postagetest.MustNewBatch()
	b.Depth = uint8(rand.Intn(10)) + depth + 3
	// random period -> random value
	period := rand.Intn(100) + 10
	value := big.NewInt(0).Mul(cs.Price, big.NewInt(int64(period)))
	value.Add(cs.Total, value)
	b.Value = big.NewInt(0)
	// add new postage batch
	return bStore.Put(b, value, b.Depth)
}

func checkReserve(bStore postage.Storer, unreserved map[string]uint8) (uint8, error) {
	var size int64
	count := 0
	min := big.NewInt(0)
	max := big.NewInt(0)
	limit, depth := batchstore.GetReserve(bStore)
	// checking all batches
	err := batchstore.IterateAll(bStore, func(b *postage.Batch) (bool, error) {
		count++
		bDepth, found := unreserved[string(b.ID)]
		if !found {
			return true, fmt.Errorf("batch not unreserved")
		}
		if b.Value.Cmp(limit) >= 0 {
			if bDepth < depth-1 || bDepth > depth {
				return true, fmt.Errorf("incorrect depth. expected %d or %d. got  %d", depth-1, depth, bDepth)
			}
			if bDepth == depth {
				if max.Cmp(b.Value) < 0 {
					max.Set(b.Value)
				}
			} else {
				if min.Cmp(b.Value) > 0 || min.Cmp(big.NewInt(0)) == 0 {
					min.Set(b.Value)
				}
			}
			if min.Cmp(big.NewInt(0)) != 0 && min.Cmp(max) <= 0 {
				return true, fmt.Errorf("inconsistent unreserve depth: %d <= %d", min.Uint64(), max.Uint64())
			}
			size += batchstore.Exp2(b.Depth - bDepth - 1)
		} else if bDepth != swarm.MaxPO {
			return true, fmt.Errorf("batch below limit expected to be fully unreserved. got found=%v, radius=%d", found, bDepth)
		}
		return false, nil
	})
	if err != nil {
		return 0, err
	}
	if size > batchstore.Capacity {
		return 0, fmt.Errorf("reserve size beyond capacity. max %d, got %d", batchstore.Capacity, size)
	}
	return depth, nil
}
