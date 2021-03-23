package telemetry

import "github.com/grafana/grafana-plugin-sdk-go/data"

type Converter interface {
	Convert(data []byte) ([]FrameWrapper, error)
}

type FrameWrapper interface {
	Key() string
	Frame() *data.Frame
}
