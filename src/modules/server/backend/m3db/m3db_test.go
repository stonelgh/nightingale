// go test -v -count=1

package m3db

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/didi/nightingale/v4/src/common/dataobj"
	"github.com/didi/nightingale/v4/src/common/stats"
	"gopkg.in/yaml.v2"
)

var strConf = `
enabled: true
maxSeriesPoints: 720 # default 720
name: "m3db"
namespace: "default"
seriesLimit: 0
docsLimit: 0
daysLimit: 7                               # max query time
# https://m3db.github.io/m3/m3db/architecture/consistencylevels/
writeConsistencyLevel: "majority"          # one|majority|all
readConsistencyLevel: "unstrict_majority"  # one|unstrict_majority|majority|all
config:
  service:
    # KV environment, zone, and service from which to write/read KV data (placement
    # and configuration). Leave these as the default values unless you know what
    # you're doing.
    env: default_env
    zone: embedded
    service: m3db
    etcdClusters:
      - zone: embedded
        endpoints:
          - 127.0.0.1:2379
`

var cli *Client

func init() {
	cfg := M3dbSection{}
	err := yaml.Unmarshal([]byte(strConf), &cfg)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	cli, err = NewClient(cfg)
	if err != nil {
		log.Fatalf("newclient err %s", err)
	}
	// defer cli.Close()

	rand.Seed(time.Now().UnixNano())
	stats.Init("n9e")
}

func TestDeviceIndependentMetrics(t *testing.T) {
	ep := "__nid__7__"
	nid := "7"
	metric := "metric_test"
	tagErrno := "errno"

	// insert device independent metrics
	metrics := []*dataobj.MetricValue{}
	now := time.Now().Unix()
	ts := now - now%10 // 对齐时间戳
	for i := 0; i < 1; i++ {
		metrics = append(metrics, &dataobj.MetricValue{
			Endpoint: ep, // not necessary; if not specified, the resulted series will have an empty endpoint
			Metric:   metric,
			Nid:      nid,
			TagsMap: map[string]string{
				// "__nid__": nid,
				tagErrno: fmt.Sprintf("tsdb.%d", i),
			},
			Value:       float64(rand.Intn(100)),
			Timestamp:   ts,
			CounterType: "GAUGE",
			Step:        10,
		})
	}
	cli.Push2Queue(metrics)
	t.Logf("inserted %v metrics", len(metrics))
	time.Sleep(time.Second) // better to delay a while. otherwise, QueryMetrics may not return the inserted metrics

	// query device independent metrics
	respMetrics := cli.QueryMetrics(dataobj.EndpointsRecv{
		// Endpoints: []string{ep},
		Nids: []string{nid},
	})
	if respMetrics == nil {
		t.Logf("QueryMetrics: nil")
	} else {
		t.Logf("QueryMetrics: %v", respMetrics.Metrics)
	}

	respdata := cli.QueryData([]dataobj.QueryData{{
		// Endpoints: []string{ep},
		Nids:       []string{nid},
		Counters:   []string{metric + "/" + tagErrno},
		ConsolFunc: "AVERAGE", // AVERAGE,MIN,MAX,LAST
		Start:      time.Now().Unix() - 3600,
		End:        time.Now().Unix(),
	}})
	// t.Logf("QueryData: %+v", respdata)
	for _, resp := range respdata {
		t.Logf("QueryData: %+v", resp)
		// for _, v := range resp.Values {
		// 	t.Logf("QueryData-Values: %+v", v)
		// }
	}

	respui := cli.QueryDataForUI(dataobj.QueryDataForUI{
		// Endpoints: []string{ep},
		Nids:       []string{nid},
		Start:      time.Now().Unix() - 3600,
		End:        time.Now().Unix(),
		Metric:     metric,
		ConsolFunc: "AVERAGE", // AVERAGE,MIN,MAX,LAST
		// Tags:       []string{},
		// GroupKey:   []string{}, // 聚合维度
		// AggrFunc:   "",         // 聚合计算: sum,avg,max,min
		// Comparisons: []int64{},  // 环比多少时间
		// DsType:      "",
	})
	// t.Logf("QueryDataForUI: %+v", respui)
	for _, resp := range respui {
		t.Logf("QueryDataForUI: %+v", resp)
		// for _, v := range resp.Values {
		// 	t.Logf("QueryDataForUI-Values: %+v", v)
		// }
	}

	resptags := cli.QueryTagPairs(dataobj.EndpointMetricRecv{
		// Endpoints: []string{ep},
		Nids:    []string{nid},
		Metrics: []string{metric},
	})
	// t.Logf("QueryTagPairs: %+v", resptags)
	for _, resp := range resptags {
		t.Logf("QueryTagPairs: %+v", resp)
		for _, tag := range resp.Tagkv {
			t.Logf("QueryTagPairs-Tagkv: %+v", tag)
		}
	}

	respIdxByClude := cli.QueryIndexByClude([]dataobj.CludeRecv{{
		// Endpoints: []string{ep},
		Nids: []string{nid},
		// Metric:  "",
		// Include: []*dataobj.TagPair{},
		// Exclude: []*dataobj.TagPair{},
	}})
	t.Logf("QueryIndexByClude: %+v", respIdxByClude)

	respidxByTags, n := cli.QueryIndexByFullTags([]dataobj.IndexByFullTagsRecv{{
		// Endpoints: []string{ep},
		Nids: []string{nid},
		// Tagkv: []dataobj.TagPair{},
	}})
	t.Logf("QueryIndexByFullTags: n=%v, %+v", n, respidxByTags)
}

/*
go test -v -count=1

{"level":"info","ts":1623987737.2707763,"msg":"waiting for dynamic topology initialization, if this takes a long time, make sure that a topology/placement is configured"}
{"level":"info","ts":1623987737.2708044,"msg":"adding a watch","service":"m3db","env":"default_env","zone":"embedded","includeUnhealthy":true}
{"level":"info","ts":1623987737.2722833,"msg":"initial topology / placement value received"}
{"level":"info","ts":1623987737.352642,"msg":"successfully updated topology","numHosts":1}
=== RUN   TestDeviceIndependentMetrics
    a_test.go:86: inserted 1 metrics
    a_test.go:97: QueryMetrics: [metric_test ping_result_code]

2021-06-18 03:42:18.432541 DEBUG m3db/m3db.go:133 query data, inputs: [{Start:1623984138 End:1623987738 ConsolFunc:AVERAGE Endpoints:[] Nids:[7] Counters:[metric_test/errno] Step:0 DsType:}]
    a_test.go:110: QueryData: &{Start:1623984138 End:1623987739 Endpoint:__nid__7__ Nid:7 Counter:metric_test/errno=tsdb.0 DsType: Step:0 Values:[<RRDData:Value:54 TS:1623985250 2021-06-18 03:00:50> <RRDData:Value:37 TS:1623985680 2021-06-18 03:08:00> <RRDData:Value:20 TS:1623985980 2021-06-18 03:13:00> <RRDData:Value:17 TS:1623986180 2021-06-18 03:16:20> <RRDData:Value:63 TS:1623986420 2021-06-18 03:20:20> <RRDData:Value:36 TS:1623986950 2021-06-18 03:29:10> <RRDData:Value:70 TS:1623987360 2021-06-18 03:36:00> <RRDData:Value:11 TS:1623987520 2021-06-18 03:38:40> <RRDData:Value:58 TS:1623987730 2021-06-18 03:42:10>]}

	2021-06-18 03:42:18.436956 DEBUG m3db/m3db.go:157 query data for ui, input: {Start:1623984138 End:1623987738 Metric:metric_test Endpoints:[] Nids:[7] Tags:[] Step:0 DsType: GroupKey:[] AggrFunc: ConsolFunc:AVERAGE Comparisons:[]}
    a_test.go:131: QueryDataForUI: &{Start:1623984138 End:1623987739 Endpoint:__nid__7__ Nid:7 Counter:metric_test/errno=tsdb.0 DsType: Step:0 Values:[<RRDData:Value:54 TS:1623985250 2021-06-18 03:00:50> <RRDData:Value:37 TS:1623985680 2021-06-18 03:08:00> <RRDData:Value:20 TS:1623985980 2021-06-18 03:13:00> <RRDData:Value:17 TS:1623986180 2021-06-18 03:16:20> <RRDData:Value:63 TS:1623986420 2021-06-18 03:20:20> <RRDData:Value:36 TS:1623986950 2021-06-18 03:29:10> <RRDData:Value:70 TS:1623987360 2021-06-18 03:36:00> <RRDData:Value:11 TS:1623987520 2021-06-18 03:38:40> <RRDData:Value:58 TS:1623987730 2021-06-18 03:42:10>]}

	a_test.go:144: QueryTagPairs: {Endpoints:[__nid__7__] Nids:[7] Metric:metric_test Tagkv:[0xc003353f80]}
    a_test.go:146: QueryTagPairs-Tagkv: &{Key:errno Values:[tsdb.0]}
    a_test.go:157: QueryIndexByClude: [{Endpoint:__nid__7__ Nid:7 Metric:ping_result_code Tags:[region=singapore,url=3.0.213.177,vendor=aws] Step:0 DsType:} {Endpoint:__nid__7__ Nid:7 Metric:metric_test Tags:[errno=tsdb.0] Step:0 DsType:}]
    a_test.go:164: QueryIndexByFullTags: n=2, [{Endpoints:[] Nids:[7] Metric: Tags:[errno=tsdb.0 region=singapore,url=3.0.213.177,vendor=aws] Step:0 DsType:GAUGE Count:2}]
--- PASS: TestDeviceIndependentMetrics (1.03s)
PASS
ok  	github.com/didi/nightingale/v4/src/modules/server/backend/m3db	1.239s
*/
