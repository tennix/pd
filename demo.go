package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"

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

	getReq = &tikv.GetRequest{Key: []byte("MaxNumber")}
	getResp, err = rawkvClient.Get(ctx, getReq)
	if err != nil {
		panic(err)
	}
	value = getResp.GetValue()
	fmt.Println("MaxNumber=", value)
	if len(value) == 0 {
		putReq = &tikv.PutRequest{Key: []byte("MaxNumber"), Value: []byte("1")}
		_, _ = rawkvClient.Put(ctx, putReq)
		putReq = &tikv.PutRequest{Key: []byte("1"), Value: []byte("1")}
		_, _ = rawkvClient.Put(ctx, putReq)
		value = []byte("1")
	} else {
		n, err := strconv.Atoi(string(value))
		if err != nil {
			panic(err)
		}
		putReq = &tikv.PutRequest{Key: []byte("MaxNumber"), Value: []byte(strconv.Itoa(n + 1))}
		_, _ = rawkvClient.Put(ctx, putReq)
		putReq = &tikv.PutRequest{Key: []byte(strconv.Itoa(n + 1)), Value: []byte("1")}
		_, _ = rawkvClient.Put(ctx, putReq)
		value = []byte(strconv.Itoa(n + 1))
	}
	scanReq := &tikv.ScanRequest{StartKey: []byte("1"), EndKey: value, Limit: 10, Desc: true}
	scanResp, err := rawkvClient.Scan(ctx, scanReq)
	if err != nil {
		panic(err)
	}
	fmt.Println("Scaned")
	for _, pair := range scanResp.Pairs {
		fmt.Println(string(pair.Key), string(pair.Value))
	}
}
