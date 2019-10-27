module github.com/pingcap/pd

go 1.12

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/chzyer/logex v1.1.10 // indirect
	github.com/chzyer/readline v0.0.0-20171208011716-f6d7a1f6fbf3
	github.com/chzyer/test v0.0.0-20180213035817-a1ea475d72b1 // indirect
	github.com/coreos/go-semver v0.2.0
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f
	github.com/dustin/go-humanize v1.0.0
	github.com/ghodss/yaml v1.0.1-0.20190212211648-25d852aebe32
	github.com/gogo/protobuf v1.0.0
	github.com/golang/protobuf v1.3.2
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c
	github.com/gorilla/mux v1.7.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/memberlist v0.1.5
	github.com/juju/ratelimit v1.0.1
	github.com/mattn/go-shellwords v1.0.3
	github.com/montanaflynn/stats v0.5.0
	github.com/opentracing/opentracing-go v1.0.2
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errcode v0.0.0-20180921232412-a1a7271709d9
	github.com/pingcap/failpoint v0.0.0-20190512135322-30cc7431d99c
	github.com/pingcap/kvproto v0.0.0-20190516013202-4cf58ad90b6c
	github.com/pingcap/log v0.0.0-20190715063458-479153f07ebd
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v0.9.2
	github.com/sirupsen/logrus v1.3.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.1
	github.com/syndtr/goleveldb v0.0.0-20180815032940-ae2bd5eed72d
	github.com/tikv/client-go v0.0.0-20190822125924-d9c03d0f448b
	github.com/unrolled/render v1.0.0
	github.com/urfave/negroni v0.3.0
	go.etcd.io/etcd v0.0.0-20190320044326-77d4b742cdbf
	go.uber.org/zap v1.9.1
	google.golang.org/grpc v1.24.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)
