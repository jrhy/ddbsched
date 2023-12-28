package ddbsched_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jrhy/ddbsched"
)

func TestCountCurrentReaders(t *testing.T) {
	t.Parallel()
	t.Run("happy", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, 2,
			ddbsched.CountCurrentReaders([]ddbsched.Snapshot{
				{CountersItem: ddbsched.StateItem{ReadersPerSamplePeriod: 2}},
				{CountersItem: ddbsched.StateItem{ReadersPerSamplePeriod: 4}},
				{CountersItem: ddbsched.StateItem{ReadersPerSamplePeriod: 6}},
				{CountersItem: ddbsched.StateItem{ReadersPerSamplePeriod: 8}},
			}))
	})
}

func TestCountCurrentWriters(t *testing.T) {
	t.Parallel()

	t.Run("happy", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, 2,
			ddbsched.CountCurrentWriters([]ddbsched.Snapshot{
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 2}},
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 4}},
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 6}},
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 8}},
			}))
	})

	t.Run("few_samples", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, 2,
			ddbsched.CountCurrentWriters([]ddbsched.Snapshot{
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 2}},
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 4}},
			}))
	})

	t.Run("no_deltas", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, 1,
			ddbsched.CountCurrentWriters([]ddbsched.Snapshot{
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 2}},
			}))
	})

	t.Run("accelerating", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, 5,
			ddbsched.CountCurrentWriters([]ddbsched.Snapshot{
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 1}},
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 3}},
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 6}},
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 10}},
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 15}},
			}))
	})

	t.Run("middlejump", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, 100,
			ddbsched.CountCurrentWriters([]ddbsched.Snapshot{
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 1}},
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 101}},
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 102}},
				{CountersItem: ddbsched.StateItem{WritersPerSamplePeriod: 103}},
			}))
	})
}

func TestBurstAvailable(t *testing.T) {
	t.Parallel()

	t.Run("happy", func(t *testing.T) {
		t.Parallel()
		require.Equal(t,
			ddbsched.AvailableCU{
				OverPeriod: time.Duration(20 * time.Second),
				RCU:        38,
				WCU:        190,
			},
			ddbsched.BurstAvailable([]ddbsched.Snapshot{
				{
					CountersItem:     ddbsched.StateItem{RCU: 1, WCU: 5},
					ProvisionedGauge: ddbsched.ProvisionedThroughput{RCU: 2, WCU: 10},
					Time:             time.Unix(0, 0),
				}, {
					CountersItem:     ddbsched.StateItem{RCU: 1, WCU: 10},
					ProvisionedGauge: ddbsched.ProvisionedThroughput{RCU: 2, WCU: 10},
					Time:             time.Unix(10, 0),
				}, {
					CountersItem:     ddbsched.StateItem{RCU: 3, WCU: 15},
					ProvisionedGauge: ddbsched.ProvisionedThroughput{RCU: 2, WCU: 10},
					Time:             time.Unix(20, 0),
				},
			}))
	})

	t.Run("starting_empty", func(t *testing.T) {
		t.Parallel()
		require.Equal(t,
			ddbsched.AvailableCU{},
			ddbsched.BurstAvailable([]ddbsched.Snapshot{
				{
					ProvisionedGauge: ddbsched.ProvisionedThroughput{WCU: 10},
				},
			}))
	})

	t.Run("starting_never_used", func(t *testing.T) {
		t.Parallel()
		require.Equal(t,
			ddbsched.AvailableCU{
				OverPeriod: time.Duration(10 * time.Second),
				RCU:        100,
				WCU:        100,
			},
			ddbsched.BurstAvailable([]ddbsched.Snapshot{
				{
					ProvisionedGauge: ddbsched.ProvisionedThroughput{RCU: 10, WCU: 10},
					Time:             time.Unix(0, 0),
				}, {
					ProvisionedGauge: ddbsched.ProvisionedThroughput{RCU: 10, WCU: 10},
					Time:             time.Unix(10, 0),
				},
			}))
	})
}

func TestRecalculate(t *testing.T) {
	t.Parallel()

	t.Run("happy", func(t *testing.T) {
		t.Parallel()
		// For a table with ProvisionedThroughput of 1 RCU and WCU/sec, that has been
		// fully utilized for half a minute, in the remaining half-minute, 2 writers can perfectly
		// consume the throughput if each spaces their writes by 4 seconds, and
		// 4 readers can perfectly consume the throughput if each spaces their reads by
		// 8 seconds.
		intervals, metrics := ddbsched.RecalculateIntervalsAndMetrics([]ddbsched.Snapshot{
			{
				CountersItem: ddbsched.StateItem{
					RCU: 0, ReadersPerSamplePeriod: 1,
					WCU: 0, WritersPerSamplePeriod: 2,
				},
				ProvisionedGauge: ddbsched.ProvisionedThroughput{RCU: 1, WCU: 1},
				Time:             time.Unix(0, 0),
			}, {
				CountersItem: ddbsched.StateItem{
					RCU: 30, ReadersPerSamplePeriod: 5,
					WCU: 30, WritersPerSamplePeriod: 4,
				},
				ProvisionedGauge: ddbsched.ProvisionedThroughput{RCU: 1, WCU: 1},
				Time:             time.Unix(60, 0),
			},
		})
		require.Equal(t, ddbsched.Intervals{
			RCUInterval: 8 * time.Second,
			WCUInterval: 4 * time.Second,
		}, intervals)
		require.Equal(t, ddbsched.Metrics{
			UnusedBurst: ddbsched.AvailableCU{
				OverPeriod: 60 * time.Second,
				RCU:        30,
				WCU:        30,
			},
			Readers: 4,
			Writers: 2,
		}, metrics)
	})

	t.Run("exhausted_read", func(t *testing.T) {
		t.Parallel()
		intervals, metrics := ddbsched.RecalculateIntervalsAndMetrics([]ddbsched.Snapshot{
			{
				CountersItem:     ddbsched.StateItem{RCU: 0},
				ProvisionedGauge: ddbsched.ProvisionedThroughput{RCU: 1},
				Time:             time.Unix(0, 0),
			}, {
				CountersItem:     ddbsched.StateItem{RCU: 90},
				ProvisionedGauge: ddbsched.ProvisionedThroughput{RCU: 1},
				Time:             time.Unix(60, 0),
			},
		})
		require.Equal(t, ddbsched.Intervals{
			RCUInterval: time.Duration(0),
		}, intervals)
		require.Equal(t, ddbsched.Metrics{
			UnusedBurst: ddbsched.AvailableCU{
				OverPeriod: 60 * time.Second,
				RCU:        0,
			},
			Readers: 1,
			Writers: 1,
		}, metrics)
	})

	t.Run("exhausted_write", func(t *testing.T) {
		t.Parallel()
		intervals, metrics := ddbsched.RecalculateIntervalsAndMetrics([]ddbsched.Snapshot{
			{
				CountersItem:     ddbsched.StateItem{WCU: 0},
				ProvisionedGauge: ddbsched.ProvisionedThroughput{WCU: 1},
				Time:             time.Unix(0, 0),
			}, {
				CountersItem:     ddbsched.StateItem{WCU: 90},
				ProvisionedGauge: ddbsched.ProvisionedThroughput{WCU: 1},
				Time:             time.Unix(60, 0),
			},
		})
		require.Equal(t, ddbsched.Intervals{
			WCUInterval: time.Duration(0),
		}, intervals)
		require.Equal(t, ddbsched.Metrics{
			UnusedBurst: ddbsched.AvailableCU{
				OverPeriod: 60 * time.Second,
				WCU:        0,
			},
			Readers: 1,
			Writers: 1,
		}, metrics)
	})

}
