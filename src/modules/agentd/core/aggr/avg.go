package aggr

import (
	"math"
	"sync"
)

type Avg struct {
	lock  sync.RWMutex
	sum   float64
	count int64
}

func (o *Avg) Add(v float64) {
	o.lock.Lock()
	defer o.lock.Unlock()
	o.sum += v
	o.count++
}

func (o *Avg) Values() map[string]float64 {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return map[string]float64{"avg": o.sum / math.Max(float64(o.count), 1)}
}
