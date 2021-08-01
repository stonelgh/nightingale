package aggr

import (
	"sync"
	"sync/atomic"

	"github.com/didi/nightingale/v4/src/common/dataobj"
)

// default step
const defStep = 60

type Aggregator interface {
	Add(float64)
	Values() map[string]float64
}

// Group is a metric series group
type Group struct {
	lock    sync.RWMutex
	series  map[string]*Series
	minStep int64
}

func New() *Group {
	return &Group{
		minStep: defStep,
		series:  make(map[string]*Series),
	}
}

func (o *Group) getSeries(m *dataobj.MetricValue) *Series {
	key := MetricKey(m)
	o.lock.RLock()
	s := o.series[key]
	o.lock.RUnlock()
	if s != nil {
		return s
	}

	o.lock.Lock()
	defer o.lock.Unlock()
	if s = o.series[key]; s != nil {
		return s
	}
	gen := GetAggrGen(m.CounterType)
	s = NewSeries(gen, m)
	o.series[key] = s
	if step := s.Step(); step < o.minStep {
		atomic.StoreInt64(&o.minStep, step)
	}
	return s
}

func (o *Group) MinStep() int64 {
	return atomic.LoadInt64(&o.minStep)
}

func (o *Group) Put(m *dataobj.MetricValue) {
	if m.Metric == "" {
		return
	}

	s := o.getSeries(m)
	s.Put(m)
}

func (o *Group) Collect() (metrics []*dataobj.MetricValue) {
	o.lock.Lock()
	defer o.lock.Unlock()
	del := []string{}
	for k, s := range o.series {
		metrics = append(metrics, s.Collect()...)
		if s.Obsolete() {
			del = append(del, k)
		}
	}
	for _, k := range del {
		delete(o.series, k)
	}
	return metrics
}

// AggrGen is a Aggregator generator
type AggrGen func() Aggregator

var aggrGens = map[string]AggrGen{}

func init() {
	RegAggr("SUM", func() Aggregator { return new(Sum) })
	RegAggr("AVG", func() Aggregator { return new(Avg) })
	RegAggr("P90", func() Aggregator { return NewQuantile(0.9) })
	RegAggr("P99", func() Aggregator { return NewQuantile(0.99) })
	RegAggr("DIVERSE", func() Aggregator { return NewDiverse() })
}

// RegAggr registers an aggregator generator of typ
func RegAggr(typ string, gen AggrGen) {
	aggrGens[typ] = gen
}

// GetAggrGen returns an aggregator generator of typ if it is valid
func GetAggrGen(typ string) AggrGen {
	return aggrGens[typ]
}

// NewAggr returns an aggregator of typ
func NewAggr(typ string) Aggregator {
	if g := GetAggrGen(typ); g != nil {
		return g()
	}
	return nil
}

// ValidAggr returns true if typ is a registered aggregator type
func ValidAggr(typ string) bool {
	g := aggrGens[typ]
	return g != nil
}
