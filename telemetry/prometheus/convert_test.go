package prometheus

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/stretchr/testify/require"
)

func loadTestData(tb testing.TB, file string) []byte {
	tb.Helper()
	content, err := ioutil.ReadFile(filepath.Join("testdata", file+".txt"))
	require.NoError(tb, err, "expected to be able to read file")
	require.True(tb, len(content) > 0)
	return content
}

func TestNewConverter(t *testing.T) {
	c := NewConverter()
	require.NotNil(t, c)
}

func TestConverter_Convert(t *testing.T) {
	testCases := []struct {
		Name      string
		NumFrames int
	}{
		{Name: "metrics", NumFrames: 93},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			testData := loadTestData(t, tt.Name)
			converter := NewConverter()
			frameWrappers, err := converter.Convert(testData)
			require.NoError(t, err)
			require.Len(t, frameWrappers, tt.NumFrames)
			for _, fw := range frameWrappers {
				frame := fw.Frame()
				_, err := data.FrameToJSON(frame, true, true)
				require.NoError(t, err)
			}
		})
	}
}

func BenchmarkConverter_Convert(b *testing.B) {
	testData := loadTestData(b, "metrics")
	converter := NewConverter()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := converter.Convert(testData)
		if err != nil {
			b.Fatal(err)
		}
	}
}
