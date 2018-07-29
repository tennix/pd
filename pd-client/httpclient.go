// Copyright 2016 PingCAP, Inc.
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

package pd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/coreos/etcd/pkg/transport"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

const (
	versionAPIPath         = "/pd/api/v1/version"
	statusAPIPath          = "/pd/api/v1/status"
	configAPIPath          = "/pd/api/v1/config"
	storesAPIPath          = "/pd/api/v1/stores"
	storeAPIPath           = "/pd/api/v1/store"
	membersAPIPath         = "/pd/api/v1/members"
	leaderMemberAPIPath    = "/pd/api/v1/leader"
	regionsAPIPath         = "/pd/api/v1/regions"
	regionAPIPath          = "/pd/api/v1/region"
	regionsCheckAPIPath    = "/pd/api/v1/regions/check"
	clusterAPIPath         = "/pd/api/v1/cluster"
	clusterStatusAPIPath   = "/pd/api/v1/cluster/status"
	schedulersAPIPath      = "/pd/api/v1/schedulers"
	hotWriteRegionsAPIPath = "/pd/api/v1/hotspot/regions/write"
	hotReadRegionsAPIPath  = "/pd/api/v1/hotspot/regions/read"
	hotStoresAPIPath       = "/pd/api/v1/hotspot/stores"
	labelsAPIPath          = "/pd/api/v1/labels"
	operatorsAPIPath       = "/pd/api/v1/operators"
	trendAPIPath           = "/pd/api/v1/trend"
	regionStatsAPIPath     = "/pd/api/v1/stats/region"
)

// HTTPClient is a PD (Placement Driver) HTTP client.
type HTTPClient interface {
	GetStatus() (*api.Status, error)
	GetVersion() (*api.Version, error)

	// GetClusterInfo gets the cluster info from PD.
	GetClusterInfo() (*metapb.Cluster, error)
	GetClusterStatus() (*server.ClusterStatus, error)

	// GetConfig
	GetConfig() (*server.Config, error)
	SetConfig() error
	GetScheduleConfig() (*server.ScheduleConfig, error)
	SetScheduleConfig(key string, value float64) error
	GetLabelPropertyConfig() (*server.LabelPropertyConfig, error)
	SetLabelPropertyConfig(typ string, key string, value string) error
	DeleteLabelPropertyConfig(typ string, key string, value string) error
	GetNamespaceConfig(name string) (*server.NamespaceConfig, error)
	SetNamespaceConfig()
	DeleteNamespaceConfig()
	GetReplicationConfig() (*server.ReplicationConfig, error)
	SetReplicationConfig(key string, value float64) error

	// GetStores gets tikv stores info from PD.
	GetStores() (*api.StoresInfo, error)
	// GetStore gets a specific tikv store info from PD.
	GetStore(id uint64) (*api.StoreInfo, error)
	SetStoreState(id uint64, state string) error
	LabelStore(id uint64, key string, value string) error
	SetStoreWeight(id uint64, leaderWeight float64, regionWeight float64) error
	DeleteStore(id uint64) error

	GetHotWriteRegions() (*core.StoreHotRegionInfos, error)
	GetHotReadRegions() (*core.StoreHotRegionInfos, error)
	GetHotStores() (*api.HotStoreStats, error)

	GetLabels() ([]*metapb.StoreLabel, error)

	// GetMembers gets pd members info from PD.
	GetMembers() (*pdpb.GetMembersResponse, error)
	SetLeaderPriority(name string, priority float64) error
	DeleteMemberByName(name string) error
	DeleteMemberByID(id uint64) error

	GetLeader() (*pdpb.Member, error)
	ResignLeader() error
	TransferLeader(string) error

	GetOperators(kind string) ([]*schedule.Operator, error)
	GetOperator(id uint64) (*schedule.Operator, error)
	AddTransferLeaderOperator(name string, regionID uint64, storeID uint64) error
	AddTransferRegionOperator(name string, regionID uint64, storeIDs ...uint64) error
	AddTransferPeerOperator(name string, regionID uint64, fromStoreID uint64, toStoreID uint64) error
	AddAddPeerOperator(name string, regionID uint64, storeID uint64) error
	AddMergeRegionOperator(name string, srcRegionID uint64, destRegionID uint64) error
	AddRemovePeerOperator(name string, regionID uint64, storeID uint64) error
	AddSplitRegionOperator(name string, regionID uint64) error
	AddScatterRegion(name string, regionID uint64) error
	DeleteOperator(regionID uint64) error

	GetSchedulers() ([]string, error)
	AddGrantLeaderScheduler(name string, storeID uint64) error
	AddEvictLeaderScheduler(name string, storeID uint64) error
	AddShuffleRegionScheduler(name string) error
	AddScatterRangeScheduler(name string, startKey string, endKey string, rangeName string) error
	DeleteScheduler(name string) error

	GetRegions() (*api.RegionsInfo, error)
	// GetSiblingRegions returns sibling regions of a given region
	GetSiblingRegions(id uint64) ([]*api.RegionInfo, error)
	GetTopWriteRegions(limit int) (*api.RegionsInfo, error)
	GetTopReadRegions(limit int) (*api.RegionsInfo, error)
	GetMissPeerRegions() ([]*core.RegionInfo, error)
	GetExtraPeerRegions() ([]*core.RegionInfo, error)
	GetPendingPeerRegions() ([]*core.RegionInfo, error)
	GetDownPeerRegions() ([]*core.RegionInfo, error)
	GetIncorrectNamespaceRegions() ([]*core.RegionInfo, error)
	GetRegionByID(id uint64) (*core.RegionInfo, error)
	GetRegionByKey(name string) (*core.RegionInfo, error)

	GetRegionStats(startKey string, endKey string) (*core.RegionStats, error)
	GetTrend() (*api.Trend, error)
}

type httpClient struct {
	http.Client
	endpoint string
}

// NewHTTPClient creates a PD HTTP client.
func NewHTTPClient(endpoint string, security SecurityOption) (HTTPClient, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	tlsInfo := transport.TLSInfo{
		CertFile:      security.CertPath,
		KeyFile:       security.KeyPath,
		TrustedCAFile: security.CAPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, err
	}
	return &httpClient{
		Client: http.Client{
			Transport: &http.Transport{TLSClientConfig: tlsConfig},
		},
		endpoint: u.String(),
	}, nil
}

func (c *httpClient) GetStatus() (*api.Status, error) {
	u := c.endpoint + statusAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	status := &api.Status{}
	if err := json.Unmarshal(resp, status); err != nil {
		return nil, err
	}
	return status, nil
}

func (c *httpClient) GetVersion() (*api.Version, error) {
	u := c.endpoint + versionAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	version := &api.Version{}
	if err := json.Unmarshal(resp, version); err != nil {
		return nil, err
	}
	return version, nil
}

func (c *httpClient) GetClusterInfo() (*metapb.Cluster, error) {
	u := c.endpoint + clusterAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	cluster := &metapb.Cluster{}
	if err := json.Unmarshal(resp, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

func (c *httpClient) GetClusterStatus() (*server.ClusterStatus, error) {
	u := c.endpoint + clusterStatusAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	status := &server.ClusterStatus{}
	if err := json.Unmarshal(resp, status); err != nil {
		return nil, err
	}
	return status, nil
}

func (c *httpClient) GetConfig() (*server.Config, error) {
	u := c.endpoint + configAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	config := &server.Config{}
	if err := json.Unmarshal(resp, config); err != nil {
		return nil, err
	}
	return config, nil
}

func (c *httpClient) GetScheduleConfig() (*server.ScheduleConfig, error) {
	u := c.endpoint + configAPIPath + "/schedule"
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	config := &server.ScheduleConfig{}
	if err := json.Unmarshal(resp, config); err != nil {
		return nil, err
	}
	return config, nil
}

func (c *httpClient) GetReplicationConfig() (*server.ReplicationConfig, error) {
	u := c.endpoint + configAPIPath + "/replicate"
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	config := &server.ReplicationConfig{}
	if err := json.Unmarshal(resp, config); err != nil {
		return nil, err
	}
	return config, nil
}

func (c *httpClient) GetNamespaceConfig(name string) (*server.NamespaceConfig, error) {
	u := fmt.Sprintf("%s%s/namespace/%s", c.endpoint, configAPIPath, name)
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	config := &server.NamespaceConfig{}
	if err := json.Unmarshal(resp, config); err != nil {
		return nil, err
	}
	return config, nil
}

func (c *httpClient) GetLabelPropertyConfig() (*server.LabelPropertyConfig, error) {
	u := c.endpoint + configAPIPath + "/label-property"
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	config := &server.LabelPropertyConfig{}
	if err := json.Unmarshal(resp, config); err != nil {
		return nil, err
	}
	return config, nil
}

func (c *httpClient) SetConfig() error {
	return nil
}

func (c *httpClient) SetScheduleConfig(key string, value float64) error {
	u := c.endpoint + configAPIPath + "/schedule"
	config := map[string]float64{
		key: value,
	}
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}
	return c.post(u, "application/json", bytes.NewBuffer(data))
}

func (c *httpClient) SetReplicationConfig(key string, value float64) error {
	u := c.endpoint + configAPIPath + "/replicate"
	config := map[string]float64{
		key: value,
	}
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}
	return c.post(u, "application/json", bytes.NewBuffer(data))
}

func (c *httpClient) SetLabelPropertyConfig(typ string, key string, value string) error {
	return c.postLabelProperty("set", typ, key, value)
}

func (c *httpClient) DeleteLabelPropertyConfig(typ string, key string, value string) error {
	return c.postLabelProperty("delete", typ, key, value)
}

func (c *httpClient) postLabelProperty(action string, typ string, key string, value string) error {
	u := c.endpoint + configAPIPath + "/label-property"
	config := map[string]string{
		"type":        typ,
		"action":      "delete",
		"label-key":   key,
		"label-value": value,
	}
	data, err := json.Marshal(config)
	if err != nil {
		return nil
	}
	return c.post(u, "application/json", bytes.NewBuffer(data))
}

func (c *httpClient) SetNamespaceConfig() {
	// TODO: unimplemented
}

func (c *httpClient) DeleteNamespaceConfig() {
	// TODO: unimplemented
}

func (c *httpClient) GetStores() (*api.StoresInfo, error) {
	u := c.endpoint + storesAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}

	stores := &api.StoresInfo{}
	if err := json.Unmarshal(resp, stores); err != nil {
		return nil, err
	}
	return stores, nil
}
func (c *httpClient) GetStore(id uint64) (*api.StoreInfo, error) {
	u := fmt.Sprintf("%s%s/%d", c.endpoint, storeAPIPath, id)
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}

	store := &api.StoreInfo{}
	if err := json.Unmarshal(resp, store); err != nil {
		return nil, err
	}
	return store, nil
}

func (c *httpClient) SetStoreState(id uint64, state string) error {
	return nil
}

func (c *httpClient) LabelStore(id uint64, key string, value string) error {
	u := fmt.Sprintf("%s%s/%d/label", c.endpoint, storeAPIPath, id)
	label := map[string]string{key: value}
	data, err := json.Marshal(label)
	if err != nil {
		return err
	}
	return c.post(u, "application/json", bytes.NewBuffer(data))
}

func (c *httpClient) SetStoreWeight(id uint64, leaderWeight float64, regionWeight float64) error {
	u := fmt.Sprintf("%s%s/%d/weight", c.endpoint, storeAPIPath, id)
	weight := map[string]float64{
		"leader": leaderWeight,
		"region": regionWeight,
	}
	data, err := json.Marshal(weight)
	if err != nil {
		return err
	}
	return c.post(u, "application/json", bytes.NewBuffer(data))
}

func (c *httpClient) DeleteStore(id uint64) error {
	u := fmt.Sprintf("%s%s/%d", c.endpoint, storeAPIPath, id)
	return c.delete(u)
}

func (c *httpClient) GetHotWriteRegions() (*core.StoreHotRegionInfos, error) {
	u := c.endpoint + hotWriteRegionsAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	infos := &core.StoreHotRegionInfos{}
	if err := json.Unmarshal(resp, infos); err != nil {
		return nil, err
	}
	return infos, nil
}

func (c *httpClient) GetHotReadRegions() (*core.StoreHotRegionInfos, error) {
	u := c.endpoint + hotReadRegionsAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	infos := &core.StoreHotRegionInfos{}
	if err := json.Unmarshal(resp, infos); err != nil {
		return nil, err
	}
	return infos, nil
}

func (c *httpClient) GetHotStores() (*api.HotStoreStats, error) {
	u := c.endpoint + hotStoresAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	stats := &api.HotStoreStats{}
	if err := json.Unmarshal(resp, stats); err != nil {
		return nil, err
	}
	return stats, nil
}

func (c *httpClient) GetLabels() ([]*metapb.StoreLabel, error) {
	u := c.endpoint + labelsAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	labels := []*metapb.StoreLabel{}
	if err := json.Unmarshal(resp, labels); err != nil {
		return nil, err
	}
	return labels, nil
}

func (c *httpClient) GetMembers() (*pdpb.GetMembersResponse, error) {
	u := c.endpoint + membersAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}

	members := &pdpb.GetMembersResponse{}
	if err := json.Unmarshal(resp, members); err != nil {
		return nil, err
	}
	return members, nil
}

func (c *httpClient) DeleteMemberByID(id uint64) error {
	u := fmt.Sprintf("%s%s/id/%d", c.endpoint, membersAPIPath, id)
	return c.delete(u)
}

func (c *httpClient) DeleteMemberByName(name string) error {
	u := fmt.Sprintf("%s%s/id/%s", c.endpoint, membersAPIPath, name)
	return c.delete(u)
}

func (c *httpClient) GetLeader() (*pdpb.Member, error) {
	u := c.endpoint + leaderMemberAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}

	member := &pdpb.Member{}
	if err := json.Unmarshal(resp, member); err != nil {
		return nil, err
	}
	return member, nil
}

func (c *httpClient) ResignLeader() error {
	u := c.endpoint + leaderMemberAPIPath + "/resign"
	return c.post(u, "", nil)
}

func (c *httpClient) TransferLeader(name string) error {
	u := fmt.Sprintf("%s%s/transfer/%s", c.endpoint, leaderMemberAPIPath, name)
	return c.post(u, "", nil)
}

func (c *httpClient) GetOperators(kind string) ([]*schedule.Operator, error) {
	u := c.endpoint + operatorsAPIPath
	if kind != "" {
		u = u + "?kind=" + kind
	}
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	operators := []*schedule.Operator{}
	if err := json.Unmarshal(resp, operators); err != nil {
		return nil, err
	}
	return operators, nil
}

func (c *httpClient) GetOperator(regionID uint64) (*schedule.Operator, error) {
	u := fmt.Sprintf("%s%s/%d", c.endpoint, operatorsAPIPath, regionID)
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	operator := &schedule.Operator{}
	if err := json.Unmarshal(resp, operator); err != nil {
		return nil, err
	}
	return operator, nil
}

func (c *httpClient) AddTransferLeaderOperator(name string, regionID uint64, storeID uint64) error {
	// TODO: unimplemented
	return nil
}
func (c *httpClient) AddTransferRegionOperator(name string, regionID uint64, storeIDs ...uint64) error {
	// TODO: unimplemented
	return nil
}
func (c *httpClient) AddTransferPeerOperator(name string, regionID uint64, fromStoreID uint64, toStoreID uint64) error {
	// TODO: unimplemented
	return nil
}
func (c *httpClient) AddAddPeerOperator(name string, regionID uint64, storeID uint64) error {
	// TODO: unimplemented
	return nil
}
func (c *httpClient) AddMergeRegionOperator(name string, srcRegionID uint64, destRegionID uint64) error {
	// TODO: unimplemented
	return nil
}
func (c *httpClient) AddRemovePeerOperator(name string, regionID uint64, storeID uint64) error {
	// TODO: unimplemented
	return nil
}
func (c *httpClient) AddSplitRegionOperator(name string, regionID uint64) error {
	// TODO: unimplemented
	return nil
}
func (c *httpClient) AddScatterRegion(name string, regionID uint64) error {
	// TODO: unimplemented
	return nil
}

func (c *httpClient) DeleteOperator(regionID uint64) error {
	u := fmt.Sprintf("%s%s/%d", c.endpoint, operatorsAPIPath, regionID)
	return c.delete(u)
}

func (c *httpClient) GetSchedulers() ([]string, error) {
	u := c.endpoint + schedulersAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	schedulers := []string{}
	if err := json.Unmarshal(resp, schedulers); err != nil {
		return nil, err
	}
	return schedulers, nil
}

func (c *httpClient) AddGrantLeaderScheduler(name string, storeID uint64) error {
	// TODO: unimplemented
	return nil
}
func (c *httpClient) AddEvictLeaderScheduler(name string, storeID uint64) error {
	// TODO: unimplemented
	return nil
}
func (c *httpClient) AddShuffleRegionScheduler(name string) error {
	// TODO: unimplemented
	return nil
}
func (c *httpClient) AddScatterRangeScheduler(name string, startKey string, endKey string, rangeName string) error {
	// TODO: unimplemented
	return nil
}

func (c *httpClient) DeleteScheduler(name string) error {
	u := fmt.Sprintf("%s%s/%s", c.endpoint, schedulersAPIPath, name)
	return c.delete(u)
}

func (c *httpClient) SetLeaderPriority(name string, priority float64) error {
	u := fmt.Sprintf("%s%s/name/%s", c.endpoint, leaderMemberAPIPath, name)
	priorities := map[string]float64{"leader-priority": priority}
	data, err := json.Marshal(priorities)
	if err != nil {
		return err
	}
	return c.post(u, "application/json", bytes.NewBuffer(data))
}

func (c *httpClient) GetRegions() (*api.RegionsInfo, error) {
	u := c.endpoint + regionsAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}

	regions := &api.RegionsInfo{}
	err = json.Unmarshal(resp, regions)
	if err != nil {
		return nil, err
	}
	return regions, nil
}

func (c *httpClient) GetSiblingRegions(id uint64) ([]*api.RegionInfo, error) {
	u := fmt.Sprintf("%s%s/sibling/%d", c.endpoint, regionsAPIPath, id)
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}

	regions := []*api.RegionInfo{}
	err = json.Unmarshal(resp, regions)
	if err != nil {
		return nil, err
	}
	return regions, nil
}

func (c *httpClient) GetTopWriteRegions(limit int) (*api.RegionsInfo, error) {
	u := c.endpoint + regionsAPIPath + "/writeflow"
	if limit > 0 {
		u = fmt.Sprintf("%s?limit=%d", u, limit)
	}
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}

	regions := &api.RegionsInfo{}
	err = json.Unmarshal(resp, regions)
	if err != nil {
		return nil, err
	}
	return regions, nil
}

func (c *httpClient) GetTopReadRegions(limit int) (*api.RegionsInfo, error) {
	u := c.endpoint + regionsAPIPath + "/readflow"
	if limit > 0 {
		u = fmt.Sprintf("%s?limit=%d", u, limit)
	}
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}

	regions := &api.RegionsInfo{}
	err = json.Unmarshal(resp, regions)
	if err != nil {
		return nil, err
	}
	return regions, nil
}

func (c *httpClient) GetMissPeerRegions() ([]*core.RegionInfo, error) {
	u := c.getRegionsCheckURL("miss-peer")
	return c.getRegions(u)
}

func (c *httpClient) GetExtraPeerRegions() ([]*core.RegionInfo, error) {
	u := c.getRegionsCheckURL("extra-peer")
	return c.getRegions(u)
}

func (c *httpClient) GetPendingPeerRegions() ([]*core.RegionInfo, error) {
	u := c.getRegionsCheckURL("pending-peer")
	return c.getRegions(u)
}

func (c *httpClient) GetDownPeerRegions() ([]*core.RegionInfo, error) {
	u := c.getRegionsCheckURL("down-peer")
	return c.getRegions(u)
}

func (c *httpClient) GetIncorrectNamespaceRegions() ([]*core.RegionInfo, error) {
	u := c.getRegionsCheckURL("incorrect-ns")
	return c.getRegions(u)
}

func (c *httpClient) GetRegionByID(id uint64) (*core.RegionInfo, error) {
	u := fmt.Sprintf("%s%s/id/%d", c.endpoint, regionAPIPath, id)
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}

	region := &core.RegionInfo{}
	err = json.Unmarshal(resp, region)
	if err != nil {
		return nil, err
	}
	return region, nil
}

func (c *httpClient) GetRegionByKey(key string) (*core.RegionInfo, error) {
	u := fmt.Sprintf("%s%s/key/%s", c.endpoint, regionAPIPath, key)
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}

	region := &core.RegionInfo{}
	err = json.Unmarshal(resp, region)
	if err != nil {
		return nil, err
	}
	return region, nil
}

func (c *httpClient) GetRegionStats(startKey string, endKey string) (*core.RegionStats, error) {
	u := c.endpoint + regionStatsAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	stats := &core.RegionStats{}
	if err := json.Unmarshal(resp, stats); err != nil {
		return nil, err
	}
	return stats, nil
}

func (c *httpClient) GetTrend() (*api.Trend, error) {
	u := c.endpoint + trendAPIPath
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	trend := &api.Trend{}
	if err := json.Unmarshal(resp, trend); err != nil {
		return nil, err
	}
	return trend, nil
}

func (c *httpClient) get(endpoint string) ([]byte, error) {
	resp, err := c.Get(endpoint)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(string(body))
	}
	return body, nil
}

func (c *httpClient) getRegionsCheckURL(state string) string {
	return c.endpoint + regionsCheckAPIPath + "/" + state
}

func (c *httpClient) getRegions(u string) ([]*core.RegionInfo, error) {
	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}
	regions := []*core.RegionInfo{}
	if err := json.Unmarshal(resp, regions); err != nil {
		return nil, err
	}
	return regions, nil
}

func (c *httpClient) delete(endpoint string) error {
	req, err := http.NewRequest(http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return errors.New(string(body))
}

func (c *httpClient) post(endpoint string, contentType string, data io.Reader) error {
	resp, err := c.Post(endpoint, contentType, data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return errors.New(string(body))
}
