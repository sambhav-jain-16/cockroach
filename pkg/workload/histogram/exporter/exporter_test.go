// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exporter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/codahale/hdrhistogram"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	mockHistogramName = "mock_histogram"
)

func TestHdrJsonExporter(t *testing.T) {
	exporter := &HdrJsonExporter{}
	t.Run("Validate", func(t *testing.T) {
		err := exporter.Validate("metrics.json")
		assert.NoError(t, err)

		err = exporter.Validate("metrics.txt")
		assert.Error(t, err)
		assert.Equal(t, "file path must end with .json", err.Error())
	})

	t.Run("Init", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.json")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)
	})

	t.Run("SnapshotAndWrite", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.json")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)

		start := time.Time{}
		elapsed := time.Second / 2
		name := mockHistogramName
		mockHistogram := hdrhistogram.New(0, 100, 1)
		err = exporter.SnapshotAndWrite(mockHistogram, start, elapsed, &name)
		require.NoError(t, err)
		err = exporter.Close(nil)
		require.NoError(t, err)

		var data map[string]interface{}
		buf, err := os.ReadFile(file.Name())
		require.NoError(t, err)
		err = json.Unmarshal(buf, &data)
		require.NoError(t, err)
		assert.Equal(t, "mock_histogram", data["Name"])

	})

	t.Run("Close", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.json")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)

		err = exporter.Close(nil)
		assert.NoError(t, err)
	})
}

func TestOpenMetricsExporter(t *testing.T) {
	exporter := &OpenMetricsExporter{}
	t.Run("Validate", func(t *testing.T) {
		err := exporter.Validate("metrics.txt")
		assert.NoError(t, err)

		err = exporter.Validate("metrics.json")
		assert.Error(t, err)
		assert.Equal(t, "file path must not end with .json", err.Error())
	})

	t.Run("Init", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.txt")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)
		assert.NotNil(t, exporter.writer)
	})

	t.Run("SnapshotAndWrite", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.txt")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)

		start := time.Time{}
		elapsed := time.Second / 2
		name := mockHistogramName
		mockHistogram := hdrhistogram.New(0, 100, 1)
		err = exporter.SnapshotAndWrite(mockHistogram, start, elapsed, &name)
		require.NoError(t, err)
		err = exporter.Close(nil)
		require.NoError(t, err)

		buf, _ := os.ReadFile(file.Name())
		assert.Contains(t, string(buf), "# TYPE")
	})

	t.Run("Close", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.txt")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)

		err = exporter.Close(nil)
		assert.NoError(t, err)
	})
}

func removeFile(file string, t *testing.T) {
	assert.NoError(t, os.Remove(file))
}

func TestOpenMetricsFileWithJson(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		var hdrSnapshot SnapshotTick
		var buf bytes.Buffer
		exporter := &OpenMetricsExporter{}
		writer := io.Writer(&buf)
		exporter.Init(&writer)

		metricLineRegex := regexp.MustCompile(`^(\w+){([^}]*)} ([\d.e+-]+) ([\d.e+-]+)$`)

		verifyOpenMetricsWithJson := func(b []byte, snapshot *SnapshotTick, histValSum float64) error {
			counts := snapshot.Hist.Counts
			countIdx := 0

			sumOfCounts := int64(0)
			for _, count := range counts {
				sumOfCounts += count
			}

			scanner := bufio.NewScanner(bytes.NewReader(b))
			sumTillNow := int64(0)
			for scanner.Scan() {
				count := counts[countIdx]
				line := scanner.Text()
				if strings.Contains(line, "# EOF") {
					return nil
				}

				if strings.HasPrefix(line, "#") {
					metric := strings.Split(line[1:], " ")
					if metric[2] != snapshot.Name || metric[3] != "histogram" {
						return errors.Errorf("invalid histogram name and type: %s", line)
					}
					continue
				}

				if matches := metricLineRegex.FindStringSubmatch(line); matches != nil {
					metricName := matches[1]
					valueStr := matches[3]
					countValue, err := strconv.ParseFloat(valueStr, 64)
					if strings.HasSuffix(metricName, "_sum") {
						if countValue != histValSum {
							return errors.Errorf("invalid histogram sum value: expected %f, go %f", histValSum, countValue)
						}
					}
					if strings.HasSuffix(metricName, "_count") {

						if err != nil {
							return err
						}
						if int64(countValue) != sumOfCounts {
							return errors.Errorf("invalid histogram count: expected %d, got %f", sumOfCounts, countValue)
						}
					}
					if int64(countValue) != sumTillNow+count {
						return errors.Errorf("invalid histogram bucket value: expected %d, got %d", sumTillNow, count)
					}

					sumTillNow += count
					countIdx++
				}
			}
			return nil
		}

		datadriven.RunTest(t, path, func(t *testing.T, data *datadriven.TestData) string {
			require.NoError(t, json.Unmarshal([]byte(data.Input), &hdrSnapshot))
			hist := hdrhistogram.Import(hdrSnapshot.Hist)
			histValSum := hist.Mean() / float64(hist.TotalCount())
			require.NoError(t, exporter.SnapshotAndWrite(hist, hdrSnapshot.Now, hdrSnapshot.Elapsed, &hdrSnapshot.Name))
			require.NoError(t, exporter.Close(nil))

			require.NoError(t, verifyOpenMetricsWithJson(buf.Bytes(), &hdrSnapshot, histValSum))
			return buf.String()
		})
	})

}
