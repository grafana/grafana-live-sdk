package frameutil

import (
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// FieldTypeFor returns a concrete type for a given interface or unknown if not known
func FieldTypeFor(t interface{}) data.FieldType {
	switch t.(type) {
	case int8:
		return data.FieldTypeInt8
	case int16:
		return data.FieldTypeInt16
	case int32:
		return data.FieldTypeInt32
	case int64:
		return data.FieldTypeInt64

	case uint8:
		return data.FieldTypeUint8
	case uint16:
		return data.FieldTypeUint16
	case uint32:
		return data.FieldTypeUint32
	case uint64:
		return data.FieldTypeUint64

	case float32:
		return data.FieldTypeFloat32
	case float64:
		return data.FieldTypeFloat64
	case bool:
		return data.FieldTypeBool
	case string:
		return data.FieldTypeString
	case time.Time:
		return data.FieldTypeTime
	}
	return data.FieldTypeUnknown
}
