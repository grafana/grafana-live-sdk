package telegraf

import (
	"fmt"
	"time"

	"github.com/grafana/grafana-live-sdk/telemetry"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/converters"
	telegraf "github.com/influxdata/line-protocol"
)

var _ telemetry.Converter = (*Converter)(nil)

// Converter converts Telegraf metrics to Grafana frames.
type Converter struct {
	parser          *telegraf.Parser
	useLabelsColumn bool
}

// ConverterOption ...
type ConverterOption func(*Converter)

// WithUseLabelsColumn ...
func WithUseLabelsColumn(enabled bool) ConverterOption {
	return func(h *Converter) {
		h.useLabelsColumn = enabled
	}
}

// NewConverter creates new Converter from Influx/Telegraf format to Grafana Data Frames.
// This converter generates one frame for each input metric name and time combination.
func NewConverter(opts ...ConverterOption) *Converter {
	c := &Converter{
		parser: telegraf.NewParser(telegraf.NewMetricHandler()),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Each unique metric frame identified by name and time.
func getFrameKey(m telegraf.Metric) string {
	return m.Name() + "_" + m.Time().String()
}

// Convert metrics.
func (c *Converter) Convert(body []byte) ([]telemetry.FrameWrapper, error) {
	metrics, err := c.parser.Parse(body)
	if err != nil {
		return nil, fmt.Errorf("error parsing metrics: %w", err)
	}
	if !c.useLabelsColumn {
		return c.convertWideFields(metrics)
	}
	return c.convertWithLabelsColumn(metrics)
}

func (c *Converter) convertWideFields(metrics []telegraf.Metric) ([]telemetry.FrameWrapper, error) {
	// maintain the order of frames as they appear in input.
	var frameKeyOrder []string
	metricFrames := make(map[string]*metricFrame)

	for _, m := range metrics {
		frameKey := getFrameKey(m)
		frame, ok := metricFrames[frameKey]
		if ok {
			// Existing frame.
			err := frame.extend(m)
			if err != nil {
				return nil, err
			}
		} else {
			frameKeyOrder = append(frameKeyOrder, frameKey)
			frame = newMetricFrame(m)
			err := frame.extend(m)
			if err != nil {
				return nil, err
			}
			metricFrames[frameKey] = frame
		}
	}

	frameWrappers := make([]telemetry.FrameWrapper, 0, len(metricFrames))
	for _, key := range frameKeyOrder {
		frameWrappers = append(frameWrappers, metricFrames[key])
	}

	return frameWrappers, nil
}

func (c *Converter) convertWithLabelsColumn(metrics []telegraf.Metric) ([]telemetry.FrameWrapper, error) {
	// maintain the order of frames as they appear in input.
	var frameKeyOrder []string
	metricFrames := make(map[string]*metricFrame)

	for _, m := range metrics {
		frameKey := m.Name()
		frame, ok := metricFrames[frameKey]
		if ok {
			// Existing frame.
			err := frame.append(m)
			if err != nil {
				return nil, err
			}
		} else {
			frameKeyOrder = append(frameKeyOrder, frameKey)
			frame = newMetricFrameLabelsColumn(m)
			err := frame.append(m)
			if err != nil {
				return nil, err
			}
			metricFrames[frameKey] = frame
		}
	}

	frameWrappers := make([]telemetry.FrameWrapper, 0, len(metricFrames))
	for _, key := range frameKeyOrder {
		frameWrappers = append(frameWrappers, metricFrames[key])
	}

	return frameWrappers, nil
}

type metricFrame struct {
	key        string
	fields     []*data.Field
	fieldCache map[string]int
}

// newMetricFrame will return a new frame with length 1.
func newMetricFrame(m telegraf.Metric) *metricFrame {
	s := &metricFrame{
		key:    m.Name(),
		fields: make([]*data.Field, 1),
	}
	s.fields[0] = data.NewField("time", nil, []time.Time{m.Time()})
	return s
}

// newMetricFrame will return a new frame with length 1.
func newMetricFrameLabelsColumn(m telegraf.Metric) *metricFrame {
	s := &metricFrame{
		key:        m.Name(),
		fields:     make([]*data.Field, 2),
		fieldCache: map[string]int{},
	}
	s.fields[0] = data.NewField("time", nil, []time.Time{})
	s.fields[1] = data.NewField("labels", nil, []string{})
	return s
}

// Key returns a key which describes Frame metrics.
func (s *metricFrame) Key() string {
	return s.key
}

// Frame transforms metricFrame to Grafana data.Frame.
func (s *metricFrame) Frame() *data.Frame {
	return data.NewFrame(s.key, s.fields...)
}

// extend existing metricFrame fields.
func (s *metricFrame) extend(m telegraf.Metric) error {
	labels := tagsToLabels(m.TagList())
	for _, f := range m.FieldList() {
		ft, v, err := getFieldTypeAndValue(f)
		if err != nil {
			return err
		}
		field := data.NewFieldFromFieldType(ft, 1)
		field.Name = f.Key
		field.Labels = labels
		field.Set(0, v)
		s.fields = append(s.fields, field)
	}
	return nil
}

func tagsToLabels(tags []*telegraf.Tag) data.Labels {
	labels := data.Labels{}
	for i := 0; i < len(tags); i += 1 {
		labels[tags[i].Key] = tags[i].Value
	}
	return labels
}

// append to existing metricFrame fields.
func (s *metricFrame) append(m telegraf.Metric) error {
	s.fields[0].Append(m.Time())
	s.fields[1].Append(tagsToLabels(m.TagList()).String()) // TODO, use labels.String()

	for _, f := range m.FieldList() {
		ft, v, err := getFieldTypeAndValue(f)
		if err != nil {
			return err
		}
		if index, ok := s.fieldCache[f.Key]; ok {
			s.fields[index].Append(v)
		} else {
			field := data.NewFieldFromFieldType(ft, 1)
			field.Name = f.Key
			field.Set(0, v)
			s.fields = append(s.fields, field)
			s.fieldCache[f.Key] = len(s.fields) - 1
		}
	}
	return nil
}

func getFieldTypeAndValue(f *telegraf.Field) (data.FieldType, interface{}, error) {
	ft := data.FieldTypeFor(f.Value)
	if ft == data.FieldTypeUnknown {
		return ft, nil, fmt.Errorf("unknown type: %t", f.Value)
	}

	// Make all fields nullable.
	ft = ft.NullableType()

	convert, ok := getConvertFunc(ft)
	if !ok {
		return ft, nil, fmt.Errorf("no converter %s=%v (%T) %s", f.Key, f.Value, f.Value, ft.ItemTypeString())
	}

	v, err := convert(f.Value)
	if err != nil {
		return ft, nil, fmt.Errorf("value convert error: %v", err)
	}
	return ft, v, nil
}

func getConvertFunc(ft data.FieldType) (func(v interface{}) (interface{}, error), bool) {
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
		return nil, false
	}
	return convert, true
}
