// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	tikv "github.com/pingcap/pd/tikv"
	"github.com/pkg/errors"

	// "github.com/tikv/client-go/key"
	// "github.com/tikv/client-go/txnkv/kv"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//revive:disable:unused-parameter

// notLeaderError is returned when current server is not the leader and not possible to process request.
// TODO: work as proxy.
var notLeaderError = status.Errorf(codes.Unavailable, "not leader")

// GetMembers implements gRPC PDServer.
func (s *Server) GetMembers(ctx context.Context, req *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	if s.etcd == nil {
		return s.pdClient.GetMembers(ctx, s.joins[0])
	}
	if s.isClosed() {
		return nil, status.Errorf(codes.Unknown, "server not started")
	}
	members, err := GetMembers(s.GetClient())
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	var etcdLeader *pdpb.Member
	leadID := s.GetEtcdLeader()
	for _, m := range members {
		if m.MemberId == leadID {
			etcdLeader = m
			break
		}
	}

	return &pdpb.GetMembersResponse{
		Header:     s.header(),
		Members:    members,
		Leader:     s.GetLeader(),
		EtcdLeader: etcdLeader,
	}, nil
}

// Tso implements gRPC PDServer.
func (s *Server) Tso(stream pdpb.PD_TsoServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		start := time.Now()
		if err = s.validateRequest(request.GetHeader()); err != nil {
			return err
		}
		count := request.GetCount()
		// TODO: get multiple TSOs
		// if s.etcd == nil {
		// 	for i := 0 ; i < maxRetryCount; i++ {
		// 		s.pdClient.GetTS
		// 	}
		// }
		ts, err := s.getRespTS(count)
		if err != nil {
			return status.Errorf(codes.Unknown, err.Error())
		}
		response := &pdpb.TsoResponse{
			Header:    s.header(),
			Timestamp: &ts,
			Count:     count,
		}
		if err := stream.Send(response); err != nil {
			return errors.WithStack(err)
		}
		tsoHandleDuration.Observe(time.Since(start).Seconds())
	}
}

// Bootstrap implements gRPC PDServer.
func (s *Server) Bootstrap(ctx context.Context, request *pdpb.BootstrapRequest) (*pdpb.BootstrapResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	if _, err := s.bootstrapCluster(request); err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb.BootstrapResponse{
		Header: s.header(),
	}, nil
}

// IsBootstrapped implements gRPC PDServer.
func (s *Server) IsBootstrapped(ctx context.Context, request *pdpb.IsBootstrappedRequest) (*pdpb.IsBootstrappedResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	if s.etcd == nil {
		// TODO: forward request to PD leader
		return &pdpb.IsBootstrappedResponse{
			Header:       s.header(),
			Bootstrapped: true,
		}, nil
	}

	cluster := s.GetRaftCluster()
	return &pdpb.IsBootstrappedResponse{
		Header:       s.header(),
		Bootstrapped: cluster != nil,
	}, nil
}

// AllocID implements gRPC PDServer.
func (s *Server) AllocID(ctx context.Context, request *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}
	if s.etcd == nil {
		// TODO: forward request to PD leader
	}

	// We can use an allocator for all types ID allocation.
	id, err := s.idAlloc.Alloc()
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb.AllocIDResponse{
		Header: s.header(),
		Id:     id,
	}, nil
}

// GetStore implements gRPC PDServer.
func (s *Server) GetStore(ctx context.Context, request *pdpb.GetStoreRequest) (*pdpb.GetStoreResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	if s.etcd == nil {
		// TODO: forward request to PD leader
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.GetStoreResponse{Header: s.notBootstrappedHeader()}, nil
	}

	store, err := cluster.GetStore(request.GetStoreId())
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}
	return &pdpb.GetStoreResponse{
		Header: s.header(),
		Store:  store.GetMeta(),
		Stats:  store.GetStoreStats(),
	}, nil
}

// checkStore2 returns an error response if the store exists and is in tombstone state.
// It returns nil if it can't get the store.
// Copied from server/command.go
func checkStore2(cluster *RaftCluster, storeID uint64) *pdpb.Error {
	store, err := cluster.GetStore(storeID)
	if err == nil && store != nil {
		if store.GetState() == metapb.StoreState_Tombstone {
			return &pdpb.Error{
				Type:    pdpb.ErrorType_STORE_TOMBSTONE,
				Message: "store is tombstone",
			}
		}
	}
	return nil
}

// PutStore implements gRPC PDServer.
func (s *Server) PutStore(ctx context.Context, request *pdpb.PutStoreRequest) (*pdpb.PutStoreResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	if s.etcd == nil {
		// TODO: forward request to PD leader
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.PutStoreResponse{Header: s.notBootstrappedHeader()}, nil
	}

	store := request.GetStore()
	if pberr := checkStore2(cluster, store.GetId()); pberr != nil {
		return &pdpb.PutStoreResponse{
			Header: s.errorHeader(pberr),
		}, nil
	}

	if err := cluster.putStore(store); err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	log.Info("put store ok", zap.Stringer("store", store))
	cluster.RLock()
	defer cluster.RUnlock()
	cluster.cachedCluster.OnStoreVersionChange()

	return &pdpb.PutStoreResponse{
		Header: s.header(),
	}, nil
}

// GetAllStores implements gRPC PDServer.
func (s *Server) GetAllStores(ctx context.Context, request *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	if s.etcd == nil {
		// TODO: forward request to PD leader
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.GetAllStoresResponse{Header: s.notBootstrappedHeader()}, nil
	}

	// Don't return tombstone stores.
	var stores []*metapb.Store
	if request.GetExcludeTombstoneStores() {
		for _, store := range cluster.GetStores() {
			if store.GetState() != metapb.StoreState_Tombstone {
				stores = append(stores, store)
			}
		}
	} else {
		stores = cluster.GetStores()
	}

	return &pdpb.GetAllStoresResponse{
		Header: s.header(),
		Stores: stores,
	}, nil
}

// StoreHeartbeat implements gRPC PDServer.
func (s *Server) StoreHeartbeat(ctx context.Context, request *pdpb.StoreHeartbeatRequest) (*pdpb.StoreHeartbeatResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	if request.GetStats() == nil {
		return nil, errors.Errorf("invalid store heartbeat command, but %v", request)
	}
	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.StoreHeartbeatResponse{Header: s.notBootstrappedHeader()}, nil
	}

	if pberr := checkStore2(cluster, request.GetStats().GetStoreId()); pberr != nil {
		return &pdpb.StoreHeartbeatResponse{
			Header: s.errorHeader(pberr),
		}, nil
	}

	cluster.RLock()
	defer cluster.RUnlock()
	err := cluster.cachedCluster.handleStoreHeartbeat(request.Stats)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb.StoreHeartbeatResponse{
		Header: s.header(),
	}, nil
}

const regionHeartbeatSendTimeout = 5 * time.Second

var errSendRegionHeartbeatTimeout = errors.New("send region heartbeat timeout")

// heartbeatServer wraps PD_RegionHeartbeatServer to ensure when any error
// occurs on Send() or Recv(), both endpoints will be closed.
type heartbeatServer struct {
	stream pdpb.PD_RegionHeartbeatServer
	closed int32
}

func (s *heartbeatServer) Send(m *pdpb.RegionHeartbeatResponse) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return io.EOF
	}
	done := make(chan error, 1)
	go func() { done <- s.stream.Send(m) }()
	select {
	case err := <-done:
		if err != nil {
			atomic.StoreInt32(&s.closed, 1)
		}
		return errors.WithStack(err)
	case <-time.After(regionHeartbeatSendTimeout):
		atomic.StoreInt32(&s.closed, 1)
		return errors.WithStack(errSendRegionHeartbeatTimeout)
	}
}

func (s *heartbeatServer) Recv() (*pdpb.RegionHeartbeatRequest, error) {
	if atomic.LoadInt32(&s.closed) == 1 {
		return nil, io.EOF
	}
	req, err := s.stream.Recv()
	if err != nil {
		atomic.StoreInt32(&s.closed, 1)
		return nil, errors.WithStack(err)
	}
	return req, nil
}

// RegionHeartbeat implements gRPC PDServer.
func (s *Server) RegionHeartbeat(stream pdpb.PD_RegionHeartbeatServer) error {
	server := &heartbeatServer{stream: stream}
	cluster := s.GetRaftCluster()
	if cluster == nil {
		resp := &pdpb.RegionHeartbeatResponse{
			Header: s.notBootstrappedHeader(),
		}
		err := server.Send(resp)
		return errors.WithStack(err)
	}

	var lastBind time.Time
	for {
		request, err := server.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}

		if err = s.validateRequest(request.GetHeader()); err != nil {
			return err
		}

		storeID := request.GetLeader().GetStoreId()
		storeLabel := strconv.FormatUint(storeID, 10)
		store, err := cluster.GetStore(storeID)
		if err != nil {
			return err
		}
		storeAddress := store.GetAddress()

		regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "recv").Inc()
		regionHeartbeatLatency.WithLabelValues(storeAddress, storeLabel).Observe(float64(time.Now().Unix()) - float64(request.GetInterval().GetEndTimestamp()))

		cluster.RLock()
		hbStreams := cluster.coordinator.hbStreams
		cluster.RUnlock()

		if time.Since(lastBind) > s.cfg.heartbeatStreamBindInterval.Duration {
			regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "bind").Inc()
			hbStreams.bindStream(storeID, server)
			lastBind = time.Now()
		}

		region := core.RegionFromHeartbeat(request)
		if region.GetLeader() == nil {
			log.Error("invalid request, the leader is nil", zap.Reflect("reqeust", request))
			continue
		}
		if region.GetID() == 0 {
			msg := fmt.Sprintf("invalid request region, %v", request)
			hbStreams.sendErr(pdpb.ErrorType_UNKNOWN, msg, request.GetLeader(), storeAddress, storeLabel)
			continue
		}

		err = cluster.HandleRegionHeartbeat(region)
		if err != nil {
			msg := err.Error()
			hbStreams.sendErr(pdpb.ErrorType_UNKNOWN, msg, request.GetLeader(), storeAddress, storeLabel)
		}

		regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "ok").Inc()
	}
}

// GetRegion implements gRPC PDServer.
func (s *Server) GetRegion(ctx context.Context, request *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}
	region, leader := cluster.GetRegionByKey(request.GetRegionKey())
	return &pdpb.GetRegionResponse{
		Header: s.header(),
		Region: region,
		Leader: leader,
	}, nil
}

// GetPrevRegion implements gRPC PDServer
func (s *Server) GetPrevRegion(ctx context.Context, request *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}

	region, leader := cluster.GetPrevRegionByKey(request.GetRegionKey())
	return &pdpb.GetRegionResponse{
		Header: s.header(),
		Region: region,
		Leader: leader,
	}, nil
}

// GetRegionByID implements gRPC PDServer.
func (s *Server) GetRegionByID(ctx context.Context, request *pdpb.GetRegionByIDRequest) (*pdpb.GetRegionResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}
	id := request.GetRegionId()
	region, leader := cluster.GetRegionByID(id)
	return &pdpb.GetRegionResponse{
		Header: s.header(),
		Region: region,
		Leader: leader,
	}, nil
}

// ScanRegions implements gRPC PDServer.
func (s *Server) ScanRegions(ctx context.Context, request *pdpb.ScanRegionsRequest) (*pdpb.ScanRegionsResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.ScanRegionsResponse{Header: s.notBootstrappedHeader()}, nil
	}
	regions := cluster.ScanRegionsByKey(request.GetStartKey(), int(request.GetLimit()))
	resp := &pdpb.ScanRegionsResponse{Header: s.header()}
	for _, r := range regions {
		leader := r.GetLeader()
		if leader == nil {
			leader = &metapb.Peer{}
		}
		resp.Regions = append(resp.Regions, r.GetMeta())
		resp.Leaders = append(resp.Leaders, leader)
	}
	return resp, nil
}

// AskSplit implements gRPC PDServer.
func (s *Server) AskSplit(ctx context.Context, request *pdpb.AskSplitRequest) (*pdpb.AskSplitResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.AskSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}
	if request.GetRegion() == nil {
		return nil, errors.New("missing region for split")
	}
	req := &pdpb.AskSplitRequest{
		Region: request.Region,
	}
	split, err := cluster.handleAskSplit(req)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb.AskSplitResponse{
		Header:      s.header(),
		NewRegionId: split.NewRegionId,
		NewPeerIds:  split.NewPeerIds,
	}, nil
}

// AskBatchSplit implements gRPC PDServer.
func (s *Server) AskBatchSplit(ctx context.Context, request *pdpb.AskBatchSplitRequest) (*pdpb.AskBatchSplitResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.AskBatchSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}

	cluster.RLock()
	defer cluster.RUnlock()
	if !cluster.cachedCluster.IsFeatureSupported(BatchSplit) {
		return &pdpb.AskBatchSplitResponse{Header: s.incompatibleVersion("batch_split")}, nil
	}
	if request.GetRegion() == nil {
		return nil, errors.New("missing region for split")
	}
	req := &pdpb.AskBatchSplitRequest{
		Region:     request.Region,
		SplitCount: request.SplitCount,
	}
	split, err := cluster.handleAskBatchSplit(req)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb.AskBatchSplitResponse{
		Header: s.header(),
		Ids:    split.Ids,
	}, nil
}

// ReportSplit implements gRPC PDServer.
func (s *Server) ReportSplit(ctx context.Context, request *pdpb.ReportSplitRequest) (*pdpb.ReportSplitResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.ReportSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}
	_, err := cluster.handleReportSplit(request)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb.ReportSplitResponse{
		Header: s.header(),
	}, nil
}

// ReportBatchSplit implements gRPC PDServer.
func (s *Server) ReportBatchSplit(ctx context.Context, request *pdpb.ReportBatchSplitRequest) (*pdpb.ReportBatchSplitResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.ReportBatchSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}

	_, err := cluster.handleBatchReportSplit(request)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb.ReportBatchSplitResponse{
		Header: s.header(),
	}, nil
}

// GetClusterConfig implements gRPC PDServer.
func (s *Server) GetClusterConfig(ctx context.Context, request *pdpb.GetClusterConfigRequest) (*pdpb.GetClusterConfigResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.GetClusterConfigResponse{Header: s.notBootstrappedHeader()}, nil
	}
	return &pdpb.GetClusterConfigResponse{
		Header:  s.header(),
		Cluster: cluster.GetConfig(),
	}, nil
}

// PutClusterConfig implements gRPC PDServer.
func (s *Server) PutClusterConfig(ctx context.Context, request *pdpb.PutClusterConfigRequest) (*pdpb.PutClusterConfigResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.PutClusterConfigResponse{Header: s.notBootstrappedHeader()}, nil
	}
	conf := request.GetCluster()
	if err := cluster.putConfig(conf); err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	log.Info("put cluster config ok", zap.Reflect("config", conf))

	return &pdpb.PutClusterConfigResponse{
		Header: s.header(),
	}, nil
}

// ScatterRegion implements gRPC PDServer.
func (s *Server) ScatterRegion(ctx context.Context, request *pdpb.ScatterRegionRequest) (*pdpb.ScatterRegionResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.ScatterRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}

	region := cluster.GetRegionInfoByID(request.GetRegionId())
	if region == nil {
		if request.GetRegion() == nil {
			return nil, errors.Errorf("region %d not found", request.GetRegionId())
		}
		region = core.NewRegionInfo(request.GetRegion(), request.GetLeader())
	}

	cluster.RLock()
	defer cluster.RUnlock()
	co := cluster.coordinator
	op, err := co.regionScatterer.Scatter(region)
	if err != nil {
		return nil, err
	}
	if op != nil {
		co.opController.AddOperator(op)
	}

	return &pdpb.ScatterRegionResponse{
		Header: s.header(),
	}, nil
}

// GetGCSafePoint implements gRPC PDServer.
func (s *Server) GetGCSafePoint(ctx context.Context, request *pdpb.GetGCSafePointRequest) (*pdpb.GetGCSafePointResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.GetGCSafePointResponse{Header: s.notBootstrappedHeader()}, nil
	}

	safePoint, err := s.kv.LoadGCSafePoint()
	if err != nil {
		return nil, err
	}

	return &pdpb.GetGCSafePointResponse{
		Header:    s.header(),
		SafePoint: safePoint,
	}, nil
}

// SyncRegions syncs the regions.
func (s *Server) SyncRegions(stream pdpb.PD_SyncRegionsServer) error {
	cluster := s.GetRaftCluster()
	if cluster == nil {
		return ErrNotBootstrapped
	}
	return s.cluster.regionSyncer.Sync(stream)
}

// UpdateGCSafePoint implements gRPC PDServer.
func (s *Server) UpdateGCSafePoint(ctx context.Context, request *pdpb.UpdateGCSafePointRequest) (*pdpb.UpdateGCSafePointResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.UpdateGCSafePointResponse{Header: s.notBootstrappedHeader()}, nil
	}

	oldSafePoint, err := s.kv.LoadGCSafePoint()
	if err != nil {
		return nil, err
	}

	newSafePoint := request.SafePoint

	// Only save the safe point if it's greater than the previous one
	if newSafePoint > oldSafePoint {
		if err := s.kv.SaveGCSafePoint(newSafePoint); err != nil {
			return nil, err
		}
		log.Info("updated gc safe point",
			zap.Uint64("safe-point", newSafePoint))
	} else if newSafePoint < oldSafePoint {
		log.Warn("trying to update gc safe point",
			zap.Uint64("old-safe-point", oldSafePoint),
			zap.Uint64("new-safe-point", newSafePoint))
		newSafePoint = oldSafePoint
	}

	return &pdpb.UpdateGCSafePointResponse{
		Header:       s.header(),
		NewSafePoint: newSafePoint,
	}, nil
}

// GetOperator gets information about the operator belonging to the speicfy region.
func (s *Server) GetOperator(ctx context.Context, request *pdpb.GetOperatorRequest) (*pdpb.GetOperatorResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb.GetOperatorResponse{Header: s.notBootstrappedHeader()}, nil
	}

	opController := cluster.coordinator.opController
	requestID := request.GetRegionId()
	r := opController.GetOperatorStatus(requestID)
	if r == nil {
		header := s.errorHeader(&pdpb.Error{
			Type:    pdpb.ErrorType_REGION_NOT_FOUND,
			Message: "Not Found",
		})
		return &pdpb.GetOperatorResponse{Header: header}, nil
	}

	return &pdpb.GetOperatorResponse{
		Header:   s.header(),
		RegionId: requestID,
		Desc:     []byte(r.Op.Desc()),
		Kind:     []byte(r.Op.Kind().String()),
		Status:   r.Status,
	}, nil
}

// validateRequest checks if Server is leader and clusterID is matched.
// TODO: Call it in gRPC intercepter.
func (s *Server) validateRequest(header *pdpb.RequestHeader) error {
	if !s.IsLeader() {
		return errors.WithStack(notLeaderError)
	}
	if header.GetClusterId() != s.clusterID {
		return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, header.GetClusterId())
	}
	return nil
}

func (s *Server) header() *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{ClusterId: s.clusterID}
}

func (s *Server) errorHeader(err *pdpb.Error) *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{
		ClusterId: s.clusterID,
		Error:     err,
	}
}

func (s *Server) notBootstrappedHeader() *pdpb.ResponseHeader {
	return s.errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_NOT_BOOTSTRAPPED,
		Message: "cluster is not bootstrapped",
	})
}

func (s *Server) incompatibleVersion(tag string) *pdpb.ResponseHeader {
	msg := fmt.Sprintf("%s incompatible with current cluster version %s", tag, s.scheduleOpt.loadClusterVersion())
	return s.errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_INCOMPATIBLE_VERSION,
		Message: msg,
	})
}

// TiKV proxy: Raw

func (s *Server) Get(ctx context.Context, req *tikv.GetRequest) (*tikv.GetResponse, error) {
	value, err := s.rawkvClient.Get(ctx, req.GetKey())
	if err != nil {
		return &tikv.GetResponse{Error: &tikv.Error{Msg: err.Error()}}, nil
	}
	return &tikv.GetResponse{Value: value}, nil
}

func (s *Server) Put(ctx context.Context, req *tikv.PutRequest) (*tikv.PutResponse, error) {
	err := s.rawkvClient.Put(ctx, req.GetKey(), req.GetValue())
	if err != nil {
		return &tikv.PutResponse{Error: &tikv.Error{Msg: err.Error()}}, nil
	}
	return &tikv.PutResponse{Error: nil}, nil
}

// TiKV proxy: Txn

func (s *Server) Transaction(stream tikv.TxnKv_TransactionServer) error {
	return nil
	// req, err := stream.Recv()
	// if err != nil {
	// 	return errors.WithStack(err)
	// }
	// if req.ReqType != tikv.TxnRequestType_Begin {
	// 	return fmt.Errorf("invalid transaction")
	// }
	// ctx := stream.Context()
	// txn, err := s.txnkvClient.Begin(ctx)
	// if err != nil {
	// 	return errors.WithStack(err)
	// }
	// for {
	// 	req, err = stream.Recv()
	// 	if err == io.EOF {
	// 		return nil
	// 	}
	// 	if err != nil {
	// 		return errors.WithStack(err)
	// 	}
	// 	var resp tikv.TxnResponse
	// 	switch req.ReqType {
	// 	case tikv.TxnRequestType_Begin:
	// 	case tikv.TxnRequestType_Get:
	// 		val, err := txn.Get(ctx, req.Get.GetKey())
	// 		if err != nil {
	// 			return errors.WithStack(err)
	// 		}
	// 		resp = tikv.TxnResponse{
	// 			Get: &tikv.TxnGetResponse{Value: val},
	// 		}

	// 	case tikv.TxnRequestType_Delete:
	// 		k := req.Delete.GetKey()
	// 		if err := txn.Delete(key.Key(k)); err != nil {
	// 			return errors.WithStack(err)
	// 		}
	// 		resp = tikv.TxnResponse{
	// 			Delete: &tikv.TxnDeleteResponse{},
	// 		}
	// 	case tikv.TxnRequestType_Put:
	// 		k := req.Put.GetKey()
	// 		v := req.Put.GetValue()
	// 		err = txn.Set(key.Key(k), v)
	// 		if err != nil {
	// 			return errors.WithStack(err)
	// 		}
	// 		resp = tikv.TxnResponse{
	// 			Put: &tikv.TxnPutResponse{},
	// 		}
	// 	case tikv.TxnRequestType_Scan:
	// 		startKey := req.Scan.GetStartKey()
	// 		endKey := req.Scan.GetEndKey()
	// 		limit := req.Scan.GetLimit()
	// 		desc := req.Scan.GetDesc()
	// 		var iter kv.Iterator
	// 		if desc { // descending order (reverse scan)
	// 			iter, err = txn.IterReverse(ctx, startKey)
	// 		} else {
	// 			iter, err = txn.Iter(ctx, startKey, endKey)
	// 		}
	// 		kvPairs := []*tikv.KvPair{}
	// 		for iter.Valid() {
	// 			if len(kvPairs) >= int(limit) {
	// 				break
	// 			}
	// 			k := iter.Key()
	// 			v := iter.Value()
	// 			kvPairs = append(kvPairs, &tikv.KvPair{Key: []byte(k), Value: v})
	// 			iter.Next(ctx)
	// 		}
	// 		resp = tikv.TxnResponse{
	// 			Scan: &tikv.TxnScanResponse{Pairs: kvPairs},
	// 		}
	// 	case tikv.TxnRequestType_Rollback:
	// 		if err := txn.Rollback(); err != nil {
	// 			return errors.WithStack(err)
	// 		}
	// 		resp = tikv.TxnResponse{
	// 			Rollback: &tikv.TxnRollbackResponse{},
	// 		}
	// 	case tikv.TxnRequestType_Commit:
	// 		if err := txn.Commit(ctx); err != nil {
	// 			return errors.WithStack(err)
	// 		}
	// 		resp = tikv.TxnResponse{
	// 			Commit: &tikv.TxnCommitResponse{},
	// 		}
	// 	case tikv.TxnRequestType_Invalid:
	// 	}
	// 	if err := stream.Send(&resp); err != nil {
	// 		return errors.WithStack(err)
	// 	}
	// }
}

func (s *Server) RunOnce(ctx context.Context, req *tikv.TxnRequest) (*tikv.TxnResponse, error) {
	return nil, nil
}
