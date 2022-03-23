package aggr

import (
	"fmt"
	"sync"

	"github.com/caio/go-tdigest"
)

type Quantile struct {
	lock sync.RWMutex
	t    *tdigest.TDigest
	q    float64
	tag  string
}

func NewQuantile(q float64) *Quantile {
	t, _ := tdigest.New()
	return &Quantile{t: t, q: q, tag: fmt.Sprintf("q=%v", q)}
}

func (o *Quantile) Add(v float64) {
	o.lock.Lock()
	defer o.lock.Unlock()
	o.t.Add(v)
}

func (o *Quantile) Values() map[string]float64 {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return map[string]float64{o.tag: o.t.Quantile(o.q)}
}
