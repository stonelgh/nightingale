package aggr

import (
	"bytes"
	"sort"

	"github.com/didi/nightingale/v4/src/common/dataobj"
)

type Metric dataobj.MetricValue

func (m *Metric) Key() string {
	buf := bytes.Buffer{}
	buf.WriteString("endpoint=")
	buf.WriteString(m.Endpoint)
	buf.WriteString(";metric=")
	buf.WriteString(m.Metric)

	var keys []string
	for k := range m.TagsMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		buf.WriteString(";")
		buf.WriteString(k)
		buf.WriteString("=")
		buf.WriteString(m.TagsMap[k])
	}
	return buf.String()
}

// func (m *Metric) AlignTs() int64 {
// 	step := m.Step
// 	if step == 0 {
// 		step = 60
// 	}
// 	return AlignTs(m.Timestamp, step)
// }

func MetricKey(m *dataobj.MetricValue) string {
	return ((*Metric)(m)).Key()
}

func AlignTs(ts, interval int64) int64 {
	return ts / interval * interval
}

// CloneTagMap returns a non-nil deep copy of the source map
func CloneTagMap(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
