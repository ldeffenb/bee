// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"errors"
	"net/http"

	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/ethersphere/bee/pkg/steward"

	"github.com/ethersphere/bee/pkg/jsonhttp"
	"github.com/ethersphere/bee/pkg/resolver"
	"github.com/gorilla/mux"
)

// StewardshipPutHandler re-uploads root hash and all of its underlying associated chunks to the network.
func (s *Service) stewardshipPutHandler(w http.ResponseWriter, r *http.Request) {
	logger := s.logger.WithName("put_stewardship").Build()

	paths := struct {
		Address swarm.Address `map:"address,resolve" validate:"required"`
	}{}
	if response := s.mapStructure(mux.Vars(r), &paths); response != nil {
		response("invalid path params", logger, w)
		return
	}

	err := s.steward.Reupload(r.Context(), paths.Address)
	if err != nil {
		logger.Debug("re-upload failed", "chunk_address", paths.Address, "error", err)
		logger.Error(nil, "re-upload failed")
		jsonhttp.InternalServerError(w, "re-upload failed")
		return
	}
	jsonhttp.OK(w, nil)
}

type trackResponse struct {
	IsRetrievable bool `json:"isRetrievable"`
	Chunks []*steward.ChunkInfo `json:"chunks"`
}

//  stewardshipTrackHandler gets detailed information about the reference and all
// associated chunks to the network.
func (s *Service) stewardshipTrackHandler(w http.ResponseWriter, r *http.Request) {
	nameOrHex := mux.Vars(r)["address"]
	address, err := s.resolveNameOrAddress(nameOrHex)
	switch {
	case errors.Is(err, resolver.ErrParse), errors.Is(err, resolver.ErrInvalidContentHash):
		s.logger.Debug("stewardship put: parse address string failed", "string", nameOrHex, "error", err)
		s.logger.Error(nil, "stewardship put: invalid address")
		jsonhttp.BadRequest(w, "invalid address")
		return
	case errors.Is(err, resolver.ErrNotFound):
		s.logger.Debug("stewardship put: address not found", "string", nameOrHex, "error", err)
		s.logger.Error(nil, "stewardship put: address not found")
		jsonhttp.NotFound(w, "address not found")
		return
	case errors.Is(err, resolver.ErrServiceNotAvailable):
		s.logger.Debug("stewardship put: service unavailable", "string", nameOrHex, "error", err)
		s.logger.Error(nil, "stewardship put: service unavailable")
		jsonhttp.InternalServerError(w, "stewardship put: resolver service unavailable")
		return
	case err != nil:
		s.logger.Debug("stewardship put: resolve address or name string failed", "string", nameOrHex, "error", err)
		s.logger.Error(nil, "stewardship put: resolve address or name string failed")
		jsonhttp.InternalServerError(w, "stewardship put: resolve name or address")
		return
	}
	res, chunks, err := s.steward.Track(r.Context(), address)
	if err != nil {
		s.logger.Debug("stewardship track: failed", "chunk_address", address, "error", err)
		s.logger.Error(nil, "stewardship track: failed")
		jsonhttp.InternalServerError(w, "stewardship track: failed")
		return
	}
	jsonhttp.OK(w, trackResponse{
		IsRetrievable: res,
		Chunks: chunks,
	})
}

type isRetrievableResponse struct {
	IsRetrievable bool `json:"isRetrievable"`
}

// stewardshipGetHandler checks whether the content on the given address is retrievable.
func (s *Service) stewardshipGetHandler(w http.ResponseWriter, r *http.Request) {
	logger := s.logger.WithName("get_stewardship").Build()

	paths := struct {
		Address swarm.Address `map:"address,resolve" validate:"required"`
	}{}
	if response := s.mapStructure(mux.Vars(r), &paths); response != nil {
		response("invalid path params", logger, w)
		return
	}

	res, err := s.steward.IsRetrievable(r.Context(), paths.Address)
	if err != nil {
		logger.Debug("is retrievable check failed", "chunk_address", paths.Address, "error", err)
		logger.Error(nil, "is retrievable")
		jsonhttp.InternalServerError(w, "is retrievable check failed")
		return
	}
	jsonhttp.OK(w, isRetrievableResponse{
		IsRetrievable: res,
	})
}
