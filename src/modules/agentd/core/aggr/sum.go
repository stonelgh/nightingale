package aggr

import (
	"sync"
)

type Sum struct {
	lock sync.RWMutex
	sum  float64
}

func (o *Sum) Add(v float64) {
	o.lock.Lock()
	defer o.lock.Unlock()
	o.sum += v
}

func (o *Sum) Values() map[string]float64 {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return map[string]float64{"sum": float64(o.sum)}
}
