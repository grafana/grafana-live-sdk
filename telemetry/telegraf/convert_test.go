package telegraf

import (
	"flag"
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

func TestConverter_Convert(t *testing.T) {
	testCases := []struct {
		Name       string
		NumMetrics int
		NumFrames  int
	}{
		{Name: "single_metric", NumMetrics: 1, NumFrames: 1},
		{Name: "same_metrics_same_labels_different_time", NumMetrics: 2, NumFrames: 2},
		{Name: "same_metrics_different_labels_different_time", NumMetrics: 2, NumFrames: 2},
		{Name: "same_metrics_different_labels_same_time", NumMetrics: 13, NumFrames: 1},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()
			testData := loadTestData(t, "single_metric")
			converter := NewConverter()
			frameWrappers, err := converter.Convert(testData)
			require.NoError(t, err)
			require.Len(t, frameWrappers, 1)
			for _, fw := range frameWrappers {
				_, err := data.FrameToJSON(fw.Frame(), true, true)
				require.NoError(t, err)
			}
		})
	}
}

var update = flag.Bool("update", false, "update golden files")

func TestConverter_Convert_NumFrameFields(t *testing.T) {
	testData := loadTestData(t, "same_metrics_different_labels_same_time")
	converter := NewConverter()
	frameWrappers, err := converter.Convert(testData)
	require.NoError(t, err)
	require.Len(t, frameWrappers, 1)
	frameWrapper := frameWrappers[0]

	goldenFile := filepath.Join("testdata", "golden.json")

	frame := frameWrapper.Frame()
	require.Len(t, frame.Fields, 131) // 10 measurements across 13 metrics + time field.
	frameJSON, err := data.FrameToJSON(frame, true, true)
	require.NoError(t, err)
	if *update {
		if err := ioutil.WriteFile(goldenFile, frameJSON, 0600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := ioutil.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	require.JSONEqf(t, string(frameJSON), string(want), "not matched with golden file")
}

func BenchmarkConverter_Convert(b *testing.B) {
	testData := loadTestData(b, "same_metrics_different_labels_same_time")
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
