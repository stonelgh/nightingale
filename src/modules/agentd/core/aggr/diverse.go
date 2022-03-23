package aggr

import (
	"math"
	"sync"

	"github.com/caio/go-tdigest"
)

type Diverse struct {
	lock  sync.RWMutex
	t     *tdigest.TDigest
	max   float64
	min   float64
	sum   float64
	count int64
}

func NewDiverse() *Diverse {
	t, _ := tdigest.New()
	return &Diverse{t: t, min: math.MaxFloat64, max: -math.MaxFloat64}
}

func (o *Diverse) Add(v float64) {
	o.lock.Lock()
	defer o.lock.Unlock()
	o.t.Add(v)
	o.sum += v
	o.count++
	if o.max < v {
		o.max = v
	}
	if o.min > v {
		o.min = v
	}
}

func (o *Diverse) Values() map[string]float64 {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return map[string]float64{
		"p50":   o.t.Quantile(0.5),
		"p90":   o.t.Quantile(0.9),
		"p99":   o.t.Quantile(0.99),
		"avg":   o.sum / math.Max(float64(o.count), 1),
		"max":   o.max,
		"min":   o.min,
		"count": float64(o.count),
	}
}
