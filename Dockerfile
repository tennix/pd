# Builder image
FROM golang:1.10.2 as builder

COPY . /go/src/github.com/pingcap/pd

RUN cd /go/src/github.com/pingcap/pd/ && make

# Executable image
FROM alpine:3.8

COPY --from=builder /go/src/github.com/pingcap/pd/bin/pd-server /pd-server

EXPOSE 2379 2380

ENTRYPOINT ["/pd-server"]
