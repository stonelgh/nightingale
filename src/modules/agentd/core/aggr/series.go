package aggr

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/didi/nightingale/v4/src/common/dataobj"
)

// Series is a metric series
type Series struct {
	lock       sync.RWMutex
	metric     dataobj.MetricValue            // metric template
	tagMetrics map[string]dataobj.MetricValue // metric template with additional tags
	genAggr    AggrGen
	aggrs      map[int64]Aggregator
	minTs      int64 // mimimal valid timestamp
}

func NewSeries(genAggr AggrGen, m *dataobj.MetricValue) *Series {
	step := m.Step
	if step <= 0 {
		step = defStep
	}
	return &Series{
		minTs:      AlignTs(time.Now().Unix()+step+3, step), // ensure the first metric point is a complete one
		genAggr:    genAggr,
		aggrs:      make(map[int64]Aggregator),
		tagMetrics: make(map[string]dataobj.MetricValue),
		metric: dataobj.MetricValue{
			Nid:         m.Nid,
			Endpoint:    m.Endpoint,
			Metric:      m.Metric,
			CounterType: "GAUGE",
			TagsMap:     m.TagsMap,
			Step:        step,
		},
	}
}

func (o *Series) Obsolete() bool {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return len(o.aggrs) == 0 && atomic.LoadInt64(&o.minTs)+o.metric.Step*10 < time.Now().Unix()
}

func (o *Series) Step() int64 {
	return o.metric.Step
}

func (o *Series) Collect() (metrics []*dataobj.MetricValue) {
	o.lock.Lock()
	defer o.lock.Unlock()
	cur := AlignTs(time.Now().Unix()-o.metric.Step/3, o.metric.Step)
	del := []int64{}
	minTs := o.minTs
	for ts, a := range o.aggrs {
		if ts < o.minTs {
			del = append(del, ts)
		} else if ts < cur {
			m := o.metric
			m.Timestamp = ts
			vals := a.Values()
			for tag, val := range vals {
				if len(vals) == 1 {
					m.Value = val
					m.ValueUntyped = val
					metrics = append(metrics, &m)
					break
				}
				tm, ok := o.tagMetrics[tag]
				if !ok {
					tm = o.metric
					tm.TagsMap = CloneTagMap(tm.TagsMap)
					tm.TagsMap["aggr"] = tag
					o.tagMetrics[tag] = tm
				}
				tm.Value = val
				tm.ValueUntyped = val
				tm.Timestamp = ts
				metrics = append(metrics, &tm)
			}
			del = append(del, ts)
			if ts > minTs {
				minTs = ts
			}
		}
	}
	atomic.StoreInt64(&o.minTs, minTs)
	for _, ts := range del {
		delete(o.aggrs, ts)
	}
	return metrics
}

func (o *Series) Put(m *dataobj.MetricValue) {
	if m.Timestamp < atomic.LoadInt64(&o.minTs) {
		return
	}
	ts := AlignTs(m.Timestamp, o.Step())
	a := o.getAggr(ts)
	a.Add(m.Value)
}

func (o *Series) getAggr(ts int64) Aggregator {
	o.lock.RLock()
	a := o.aggrs[ts]
	o.lock.RUnlock()
	if a != nil {
		return a
	}

	o.lock.Lock()
	defer o.lock.Unlock()
	if a = o.aggrs[ts]; a != nil {
		return a
	}
	a = o.genAggr()
	o.aggrs[ts] = a
	return a
}
