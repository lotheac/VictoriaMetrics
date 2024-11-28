package storage

import (
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
)

// The maximum length of label name.
//
// Samples with longer names are ignored.
const maxLabelNameLen = 256

var (
	// The maximum number of labels per each timeseries.
	//
	// Samples with exceeding amount of labels are ignored.
	maxLabelsPerTimeseries = 40

	// The maximum length of label value.
	//
	// Samples with longer label values are ignored.
	maxLabelValueLen = 4 * 1024
)

// InitLabelsLimits inits labels limits and metrics for them
func InitLabelsLimits(inputMaxLabelsPerTimeseries, inputMaxLabelValueLen int) {
	maxLabelsPerTimeseries = inputMaxLabelsPerTimeseries
	maxLabelValueLen = inputMaxLabelValueLen

	_ = metrics.NewGauge(`vm_rows_ignored_total{reason="too_many_labels"}`, func() float64 {
		return float64(ignoredSeriesWithTooManyLabels.Load())
	})
	_ = metrics.NewGauge(`vm_rows_ignored_total{reason="too_long_label_name"}`, func() float64 {
		return float64(ignoredSeriesWithTooLongLabelName.Load())
	})
	_ = metrics.NewGauge(`vm_rows_ignored_total{reason="too_long_label_value"}`, func() float64 {
		return float64(ignoredSeriesWithTooLongLabelValue.Load())
	})
}

var (
	ignoredSeriesWithTooManyLabelsLogTicker     = time.NewTicker(5 * time.Second)
	ignoredSeriesWithTooLongLabelNameLogTicker  = time.NewTicker(5 * time.Second)
	ignoredSeriesWithTooLongLabelValueLogTicker = time.NewTicker(5 * time.Second)
)

var (
	// ignoredSeriesWithTooManyLabels is the number of ignored series with too many labels
	ignoredSeriesWithTooManyLabels atomic.Uint64

	// ignoredSeriesWithTooLongLabelName is the number of ignored series which contain labels with too long names
	ignoredSeriesWithTooLongLabelName atomic.Uint64

	// ignoredSeriesWithTooLongLabelValue is the number of ignored series which contain labels with too long values
	ignoredSeriesWithTooLongLabelValue atomic.Uint64
)

func trackIgnoredSeriesWithTooManyLabels(labels []prompbmarshal.Label) {
	ignoredSeriesWithTooManyLabels.Add(1)
	select {
	case <-ignoredSeriesWithTooManyLabelsLogTicker.C:
		// Do not call logger.WithThrottler() here, since this will result in increased CPU usage
		// because prompbmarshal.LabelsToString() will be called with each trackIgnoredSeriesWithTooManyLabels call.
		logger.Warnf("ignoring series with %d labels for %s; either reduce the number of labels for this metric "+
			"or increase -maxLabelsPerTimeseries=%d command-line flag value",
			len(labels), prompbmarshal.LabelsToString(labels), maxLabelsPerTimeseries)
	default:
	}
}

func trackIgnoredSeriesWithTooLongLabelValue(l *prompbmarshal.Label, labels []prompbmarshal.Label) {
	ignoredSeriesWithTooLongLabelValue.Add(1)
	select {
	case <-ignoredSeriesWithTooLongLabelValueLogTicker.C:
		label := *l
		// Do not call logger.WithThrottler() here, since this will result in increased CPU usage
		// because prompbmarshal.LabelsToString() will be called with each trackIgnoredSeriesWithTooLongLabelValue call.
		logger.Warnf("ignoring series with %s=%q label for %s; label value length=%d exceeds -maxLabelValueLen=%d; "+
			"either reduce the label value length or increase -maxLabelValueLen command-line flag value",
			label.Name, label.Value, prompbmarshal.LabelsToString(labels), len(label.Value), maxLabelValueLen)
	default:
	}
}

func trackIgnoredSeriesWithTooLongLabelName(l *prompbmarshal.Label, labels []prompbmarshal.Label) {
	ignoredSeriesWithTooLongLabelName.Add(1)
	select {
	case <-ignoredSeriesWithTooLongLabelNameLogTicker.C:
		label := *l
		// Do not call logger.WithThrottler() here, since this will result in increased CPU usage
		// because prompbmarshal.LabelsToString() will be called with each trackIgnoredSeriesWithTooLongLabelName call.
		logger.Warnf("ignoring series with label %q for %s; label name length=%d exceeds max allowed %d - consider reducing label name length.",
			label.Name, prompbmarshal.LabelsToString(labels), len(label.Name), maxLabelNameLen)
	default:
	}
}

// ExceedingLabels checks if passed labels exceed one of the limits:
// * Maximum allowed labels limit
// * Maximum allowed label name length limit
// * Maximum allowed label value length limit
//
// increments metrics and shows warning in logs
func ExceedingLabels(labels []prompbmarshal.Label) bool {
	if len(labels) > maxLabelsPerTimeseries {
		trackIgnoredSeriesWithTooManyLabels(labels)
		return true
	}
	for _, l := range labels {
		if len(l.Name) > maxLabelNameLen {
			trackIgnoredSeriesWithTooLongLabelName(&l, labels)
			return true
		}
		if len(l.Value) > maxLabelValueLen {
			trackIgnoredSeriesWithTooLongLabelValue(&l, labels)
			return true
		}
	}
	return false
}
