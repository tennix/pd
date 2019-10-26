#!/usr/bin/env bash

clientPort=2379
peerPort=2380
tikvPort=20160
statusPort=20180
if [ -z $1 ]; then
    echo "Usage: ./start-pd-cluster.sh N"
    exit 0
elif [ $1 == "clean" ]; then
    echo "Clean old data..."
    rm -rf pd*-data tikv*-data *.log
elif [ $1 == "0" ]; then
    name=pd${1}
    bin/pd-server --data-dir=pd${1}-data --name=$name --peer-urls=http://0.0.0.0:${peerPort} --advertise-peer-urls=http://127.0.0.1:${peerPort} --client-urls=http://0.0.0.0:${clientPort} --advertise-client-urls=http://127.0.0.1:${clientPort} --initial-cluster=pd${1}=http://127.0.0.1:${peerPort} --log-file=pd${1}.log &
    echo "Waiting for PD ready..."
    sleep 20
    tikv-server --pd=127.0.0.1:${clientPort} --advertise-addr=127.0.0.1:${tikvPort} --addr=0.0.0.0:${tikvPort} --status-addr=0.0.0.0:${statusPort} --data-dir=tikv${1}-data --log-file=tikv${1}.log &
else
    name=pd${1}
    let clientPort=$clientPort+100*$1
    let peerPort=$peerPort+100*$1
    let tikvPort=$tikvPort+100*$1
    let statusPort=$statusPort+100*$1
    bin/pd-server --data-dir=pd${1}-data --name=$name --peer-urls=http://0.0.0.0:${peerPort} --advertise-peer-urls=http://127.0.0.1:${peerPort} --client-urls=http://0.0.0.0:${clientPort} --advertise-client-urls=http://127.0.0.1:${clientPort} --join=http://127.0.0.1:2379 --log-file=pd${1}.log &
    echo "Waiting for PD ready..."
    sleep 20
    tikv-server --pd=127.0.0.1:${clientPort} --advertise-addr=127.0.0.1:${tikvPort} --addr=0.0.0.0:${tikvPort} --status-addr=0.0.0.0:${statusPort} --data-dir=tikv${1}-data --log-file=tikv${1}.log &
fi
