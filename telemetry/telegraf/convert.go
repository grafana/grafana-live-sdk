package telegraf

import (
	"fmt"
	"time"

	"github.com/grafana/grafana-live-sdk/internal/frameutil"
	"github.com/grafana/grafana-live-sdk/telemetry"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/converters"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/parsers"
	"github.com/influxdata/telegraf/plugins/parsers/influx"
)

var _ telemetry.Converter = (*Converter)(nil)

// Converter converts Telegraf metrics to Grafana frames.
type Converter struct {
	parser parsers.Parser
}

// NewConverter creates new Converter.
func NewConverter() *Converter {
	return &Converter{
		parser: influx.NewParser(influx.NewMetricHandler()),
	}
}

// Each unique metric frame identified by name and time.
func getMetricFrameKey(m telegraf.Metric) string {
	return m.Name() + "_" + m.Time().String()
}

// Convert metrics.
func (c *Converter) Convert(body []byte) ([]telemetry.FrameWrapper, error) {
	metrics, err := c.parser.Parse(body)
	if err != nil {
		return nil, fmt.Errorf("error parsing metrics: %w", err)
	}

	metricFrames := make(map[string]*MetricFrame)

	for _, m := range metrics {
		var metricFrame *MetricFrame
		var ok bool
		batchKey := getMetricFrameKey(m)
		metricFrame, ok = metricFrames[batchKey]
		if ok {
			// Existing time frame.
			err := metricFrame.extend(m)
			if err != nil {
				return nil, err
			}
		} else {
			var err error
			metricFrame, err = newMetricFrame(m)
			if err != nil {
				continue
			}
			err = metricFrame.extend(m)
			if err != nil {
				return nil, err
			}
			metricFrames[batchKey] = metricFrame
		}
	}

	frameWrappers := make([]telemetry.FrameWrapper, 0, len(metricFrames))
	for _, metricFrame := range metricFrames {
		frameWrappers = append(frameWrappers, metricFrame)
	}

	return frameWrappers, nil
}

type MetricFrame struct {
	key    string
	fields []*data.Field
}

// newMetricFrame will return a new frame with length 1.
func newMetricFrame(m telegraf.Metric) (*MetricFrame, error) {
	s := &MetricFrame{
		key:    m.Name(),
		fields: make([]*data.Field, 1),
	}
	s.fields[0] = data.NewField("time", nil, []time.Time{m.Time()})
	return s, nil
}

// Frame transforms MetricFrame to Grafana data.Frame.
func (s *MetricFrame) Key() string {
	return s.key
}

// Frame transforms MetricFrame to Grafana data.Frame.
func (s *MetricFrame) Frame() *data.Frame {
	return data.NewFrame(s.key, s.fields...)
}

// extend existing MetricFrame fields.
func (s *MetricFrame) extend(m telegraf.Metric) error {
	for _, f := range m.FieldList() {
		ft := frameutil.FieldTypeFor(f.Value)
		if ft == data.FieldTypeUnknown {
			return fmt.Errorf("unknown type: %t", f.Value)
		}

		// Make all fields nullable.
		ft = ft.NullableType()

		// NOTE (FZambia): field pool?
		field := data.NewFieldFromFieldType(ft, 1)
		field.Name = f.Key
		field.Labels = m.Tags()

		var convert func(v interface{}) (interface{}, error)

		switch ft {
		case data.FieldTypeNullableString:
			convert = converters.AnyToNullableString.Converter
		case data.FieldTypeNullableFloat64:
			convert = converters.JSONValueToNullableFloat64.Converter
		case data.FieldTypeNullableBool:
			convert = converters.BoolToNullableBool.Converter
		case data.FieldTypeNullableInt64:
			convert = converters.JSONValueToNullableInt64.Converter
		default:
			return fmt.Errorf("no converter %s=%v (%T) %s\n", f.Key, f.Value, f.Value, ft.ItemTypeString())
		}

		if v, err := convert(f.Value); err == nil {
			field.Set(0, v)
		}
		s.fields = append(s.fields, field)
	}
	return nil
}
