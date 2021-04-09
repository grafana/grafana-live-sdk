package prometheus

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/grafana/grafana-live-sdk/telemetry"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

var _ telemetry.Converter = (*Converter)(nil)

// Converter converts Prometheus metrics to Grafana frames.
type Converter struct {
	format expfmt.Format
}

// NewConverter creates Converter.
func NewConverter() *Converter {
	return &Converter{
		format: expfmt.FmtText,
	}
}

func (c Converter) metricsToFrameWrappers(mfs []*dto.MetricFamily) ([]telemetry.FrameWrapper, error) {
	var frameWrappers []telemetry.FrameWrapper
	for _, mf := range mfs {
		metricType := mf.GetType()
		metricName := mf.GetName()
		fields := make([]*data.Field, 3)
		fields[0] = data.NewField("time", nil, []time.Time{})
		fields[1] = data.NewField("label", nil, []string{})
		switch metricType {
		case dto.MetricType_COUNTER, dto.MetricType_SUMMARY, dto.MetricType_GAUGE:
			fields[2] = data.NewField("value", nil, []float64{})
		case dto.MetricType_HISTOGRAM:
			fields[2] = data.NewField("value", nil, []uint64{})
		}
		for _, m := range mf.GetMetric() {
			// TODO: handle m.GetTimestampMs()
			switch metricType {
			case dto.MetricType_COUNTER:
				counter := m.GetCounter()
				fields[0].Append(time.Now())
				fields[1].Append(labelsString(m.Label))
				fields[2].Append(counter.GetValue())
			case dto.MetricType_GAUGE:
				gauge := m.GetGauge()
				fields[0].Append(time.Now())
				fields[1].Append(labelsString(m.Label))
				fields[2].Append(gauge.GetValue())
			case dto.MetricType_SUMMARY:
				summary := m.GetSummary()
				// TODO: sum and count.
				// count := summary.GetSampleCount()
				// sum := summary.GetSampleSum()
				for _, q := range summary.GetQuantile() {
					fields[0].Append(time.Now())
					fields[1].Append(labelsString(extendQuantileLabels(q, m.GetLabel())))
					fields[2].Append(q.GetValue())
				}
			case dto.MetricType_HISTOGRAM:
				hist := m.GetHistogram()
				// TODO: sum and count.
				// count := hist.GetSampleCount()
				// sum := hist.GetSampleSum()
				for _, b := range hist.GetBucket() {
					b.GetCumulativeCount()
					fields[0].Append(time.Now())
					fields[1].Append(labelsString(extendBucketLabels(b, m.GetLabel())))
					fields[2].Append(b.GetCumulativeCount())
				}
			}
		}
		frame := data.NewFrame(metricName, fields...)
		frameWrappers = append(frameWrappers, &metricFrame{key: metricName, frame: frame})
	}
	return frameWrappers, nil
}

// Convert input to data frames.
func (c Converter) Convert(input []byte) ([]telemetry.FrameWrapper, error) {
	decoder := expfmt.NewDecoder(bytes.NewReader(input), c.format)
	var mfs []*dto.MetricFamily
	for {
		var mf dto.MetricFamily
		err := decoder.Decode(&mf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		mfs = append(mfs, &mf)
	}
	return c.metricsToFrameWrappers(mfs)
}

func floatToString(val float64) string {
	// TODO: better string representation.
	return fmt.Sprintf("%f", val)
}

func extendQuantileLabels(q *dto.Quantile, labels []*dto.LabelPair) []*dto.LabelPair {
	labelsCopy := make([]*dto.LabelPair, len(labels))
	copy(labelsCopy, labels)
	name := "quantile"
	value := floatToString(q.GetQuantile())
	labelsCopy = append(labelsCopy, &dto.LabelPair{Name: &name, Value: &value})
	return labelsCopy
}

func extendBucketLabels(q *dto.Bucket, labels []*dto.LabelPair) []*dto.LabelPair {
	labelsCopy := make([]*dto.LabelPair, len(labels))
	copy(labelsCopy, labels)
	name := "le"
	value := floatToString(q.GetUpperBound())
	labelsCopy = append(labelsCopy, &dto.LabelPair{Name: &name, Value: &value})
	return labelsCopy
}

type metricFrame struct {
	key   string
	frame *data.Frame
}

// Key returns a key which describes Frame metrics.
func (s *metricFrame) Key() string {
	return s.key
}

// Frame transforms metricFrame to Grafana data.Frame.
func (s *metricFrame) Frame() *data.Frame {
	return s.frame
}

func labelsString(l []*dto.LabelPair) string {
	sort.Slice(l, func(i, j int) bool {
		return l[i].GetName() < l[j].GetName()
	})
	var sb strings.Builder
	for i, k := range l {
		sb.WriteString(k.GetName())
		sb.WriteString("=")
		sb.WriteString(k.GetValue())
		if i != len(l)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}
