package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/pingcap/kvproto/pkg/pdpb"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/pd/pkg/grpcutil"
	tikv "github.com/pingcap/pd/tikv"
)

var addr string

func init() {
	flag.StringVar(&addr, "addr", "http://127.0.0.1:2379", "pd address")
	flag.Parse()
}

func main() {
	ctx, _ := context.WithCancel(context.Background())
	conn, err := grpcutil.GetClientConn(addr, "", "", "")
	if err != nil {
		panic(err)
	}
	members, err := pdpb.NewPDClient(conn).GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		panic(err)
	}
	for _, member := range members.Members {
		fmt.Println("members:", member)
	}

	client, err := pd.NewClient([]string{addr}, pd.SecurityOption{})
	if err != nil {
		panic(err)
	}
	resp, err := client.GetMembers(ctx, addr)
	if err != nil {
		panic(err)
	}
	for _, member := range resp.Members {
		fmt.Println("members:", member)
	}

	pt, lt, err := client.GetTS(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("PT: %d, LT: %d\n", pt, lt)

	storeResp, err := client.GetAllStores(ctx)
	if err != nil {
		panic(err)
	}
	for _, store := range storeResp {
		fmt.Println("Store:", store)
	}

	rawkvClient := tikv.NewRawKvClient(conn)
	getReq := &tikv.GetRequest{Key: []byte("Company")}
	getResp, err := rawkvClient.Get(ctx, getReq)
	if err != nil {
		panic(err)
	}
	value := getResp.GetValue()
	fmt.Printf("Company: %s\n", value)

	var putReq *tikv.PutRequest
	if len(getResp.GetValue()) == 0 {
		putReq = &tikv.PutRequest{Key: []byte("Company"), Value: []byte("PingCAP")}
	} else {
		val := string(value) + string(value)
		putReq = &tikv.PutRequest{Key: []byte("Company"), Value: []byte(val)}
	}
	_, err = rawkvClient.Put(ctx, putReq)
	if err != nil {
		panic(err)
	}
	fmt.Println("Put success")

	getResp, err = rawkvClient.Get(ctx, getReq)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Company: %s\n", getResp.GetValue())
}
