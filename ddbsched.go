package ddbsched

import (
	"math"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type Snapshot struct {
	Time             time.Time
	ProvisionedGauge ProvisionedThroughput
	CountersItem     StateItem
}

type StateItem struct {
	RCU                    uint64
	WCU                    uint64
	ReadersPerSamplePeriod uint64
	WritersPerSamplePeriod uint64
}

type ProvisionedThroughput struct {
	RCU uint64
	WCU uint64
}

type AvailableCU struct {
	OverPeriod time.Duration
	RCU        uint64
	WCU        uint64
}

type Persister interface {
	GetProvisionedThroughput() (ProvisionedThroughput, bool)
	UpdateCountersItem(updateExpression string, eavs map[string]uint64) (StateItem, bool)
	UpdateMetrics(Metrics, ProvisionedThroughput)
}

type sched struct {
	BurstPeriod           time.Duration
	SamplesPerBurstPeriod int

	Done            chan struct{}
	Persister       Persister
	RefreshInterval time.Duration

	Lock         sync.Mutex
	BatchRCURate *rate.Limiter
	BatchWCURate *rate.Limiter
	LocalRCU     uint64
	LocalWCU     uint64
	Samples      []Snapshot
}

func New(p Persister, refreshInterval time.Duration) *sched {

	res := &sched{
		BurstPeriod:           300 * time.Second,
		SamplesPerBurstPeriod: int(math.Round(300_000.0 / float64(refreshInterval.Milliseconds()))),

		Done:            make(chan struct{}, 1),
		Persister:       p,
		RefreshInterval: refreshInterval,

		BatchWCURate: &rate.Limiter{},
		Samples:      make([]Snapshot, 0),
	}
	go refreshLoop(res)
	return res
}

func (s *sched) Close() {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if s.Done == nil {
		return
	}
	s.Done <- struct{}{}
	s.Done = nil
}

func refreshLoop(s *sched) {
	for {
		select {
		case <-s.Done:
			return
		case <-time.After(s.RefreshInterval):
			s.refresh()
		}
	}
}

func (s *sched) UsedWCU(n uint32) {
	s.Lock.Lock()
	s.LocalWCU += uint64(n)
	s.Lock.Unlock()
}

// WantWCU tells whether the available burst capacity can accommodate the given number of
// capacity units. If used, the caller should call UsedWCU() with the used amount.
func (s *sched) WantWCU(n uint32) bool {
	return !s.BatchWCURate.AllowN(time.Now(), int(n))
}

func (s *sched) refresh() {
	pt, ok := s.Persister.GetProvisionedThroughput()
	if !ok {
		// try again later
		return
	}
	s.Lock.Lock()
	incRCU := s.LocalRCU
	incWCU := s.LocalWCU
	s.LocalRCU = 0
	s.LocalWCU = 0
	s.Lock.Unlock()

	si, ok := s.Persister.UpdateCountersItem("ADD RCU :incrcu, WCU :incwcu, ReadersPerSamplePeriod :one, WritersPerSamplePeriod :one",
		map[string]uint64{
			":incrcu": incRCU,
			":incwcu": incWCU,
			":one":    1,
		})
	if !ok {
		// try again later
		s.Lock.Lock()
		s.LocalRCU += incRCU
		s.LocalWCU += incWCU
		s.Lock.Unlock()
		return
	}
	t := time.Now()
	s.Lock.Lock()
	defer s.Lock.Unlock()
	s.Samples = append(s.Samples, Snapshot{
		ProvisionedGauge: pt,
		CountersItem:     si,
		Time:             t,
	})
	metrics := s.updateTargets()
	s.Persister.UpdateMetrics(metrics, pt)
}

func (s *sched) updateTargets() Metrics {
	if len(s.Samples) > s.SamplesPerBurstPeriod {
		s.Samples = s.Samples[len(s.Samples)-s.SamplesPerBurstPeriod:]
	}
	intervals, metrics := RecalculateIntervalsAndMetrics(s.Samples)
	if intervals.RCUInterval.Nanoseconds() == 0 {
		s.BatchRCURate = &rate.Limiter{}
	} else {
		s.BatchRCURate = rate.NewLimiter(rate.Every(intervals.RCUInterval), int(s.BurstPeriod/intervals.RCUInterval)/metrics.Readers+1)
	}
	if intervals.WCUInterval.Nanoseconds() == 0 {
		s.BatchWCURate = &rate.Limiter{}
	} else {
		s.BatchWCURate = rate.NewLimiter(rate.Every(intervals.WCUInterval), int(s.BurstPeriod/intervals.WCUInterval)/metrics.Writers+1)
	}
	return metrics
}

type Intervals struct {
	RCUInterval time.Duration
	WCUInterval time.Duration
}

type Metrics struct {
	UnusedBurst AvailableCU
	Readers     int
	Writers     int
}

func RecalculateIntervalsAndMetrics(ss []Snapshot) (Intervals, Metrics) {
	burstAvailable := BurstAvailable(ss)
	readers := CountCurrentReaders(ss)
	writers := CountCurrentWriters(ss)
	metrics := Metrics{
		UnusedBurst: burstAvailable,
		Readers:     readers,
		Writers:     writers,
	}
	var intervals Intervals
	if burstAvailable.RCU <= 0 || readers == 0 {
		intervals.RCUInterval = time.Duration(0)
	} else {
		intervals.RCUInterval = burstAvailable.OverPeriod * time.Duration(readers) / time.Duration(burstAvailable.RCU)
	}
	if burstAvailable.WCU <= 0 || writers == 0 {
		intervals.WCUInterval = time.Duration(0)
	} else {
		intervals.WCUInterval = burstAvailable.OverPeriod * time.Duration(writers) / time.Duration(burstAvailable.WCU)
	}
	return intervals, metrics
}

// CountCurrentReaders returns the number of readers sharing the table capacity,
// as determined by looking at the 3 most recent deltas.
func CountCurrentReaders(ss []Snapshot) int {
	var subsequentReadersSample uint64
	validDeltas := 0
	maxReaders := 1
	for i := len(ss) - 1; i >= 0; i-- {
		if ss[i].CountersItem.ReadersPerSamplePeriod == 0 {
			subsequentReadersSample = 0
			continue
		}
		if subsequentReadersSample < ss[i].CountersItem.ReadersPerSamplePeriod {
			subsequentReadersSample = ss[i].CountersItem.ReadersPerSamplePeriod
			continue
		}
		validDeltas++
		curReaders := int(subsequentReadersSample - ss[i].CountersItem.ReadersPerSamplePeriod)
		if curReaders > 0 && maxReaders < curReaders {
			maxReaders = curReaders
		}
		if validDeltas >= 3 {
			return maxReaders
		}
		subsequentReadersSample = ss[i].CountersItem.ReadersPerSamplePeriod
	}
	return maxReaders
}

// CountCurrentWriters returns the number of writers sharing the table capacity,
// as determined by looking at the 3 most recent deltas.
func CountCurrentWriters(ss []Snapshot) int {
	var subsequentWritersSample uint64
	validDeltas := 0
	maxWriters := 1
	for i := len(ss) - 1; i >= 0; i-- {
		if ss[i].CountersItem.WritersPerSamplePeriod == 0 {
			subsequentWritersSample = 0
			continue
		}
		if subsequentWritersSample < ss[i].CountersItem.WritersPerSamplePeriod {
			subsequentWritersSample = ss[i].CountersItem.WritersPerSamplePeriod
			continue
		}
		validDeltas++
		curWriters := int(subsequentWritersSample - ss[i].CountersItem.WritersPerSamplePeriod)
		if curWriters > 0 && maxWriters < curWriters {
			maxWriters = curWriters
		}
		if validDeltas >= 3 {
			return maxWriters
		}
		subsequentWritersSample = ss[i].CountersItem.WritersPerSamplePeriod
	}
	return maxWriters
}

func BurstAvailable(ss []Snapshot) AvailableCU {
	var last *Snapshot
	var periodDuration time.Duration
	var periodProvisionedRCU uint64
	var periodUsedRCU uint64
	var periodProvisionedWCU uint64
	var periodUsedWCU uint64
	for i := range ss {
		s := ss[i]
		if last == nil {
			last = &s
			continue
		}
		d := delta(last, &s)
		periodDuration += d.Duration
		periodProvisionedRCU += uint64(math.Round(float64(ss[i].ProvisionedGauge.RCU) * d.Duration.Seconds()))
		periodUsedRCU += d.StateItem.RCU
		periodProvisionedWCU += uint64(math.Round(float64(ss[i].ProvisionedGauge.WCU) * d.Duration.Seconds()))
		periodUsedWCU += d.StateItem.WCU
		last = &s
	}
	if periodUsedRCU > periodProvisionedRCU {
		periodUsedRCU = periodProvisionedRCU
	}
	if periodUsedWCU > periodProvisionedWCU {
		periodUsedWCU = periodProvisionedWCU
	}
	return AvailableCU{
		OverPeriod: periodDuration,
		RCU:        periodProvisionedRCU - periodUsedRCU,
		WCU:        periodProvisionedWCU - periodUsedWCU,
	}
}

type SampleDelta struct {
	Duration  time.Duration
	StateItem StateItem
}

func delta(a, b *Snapshot) SampleDelta {
	return SampleDelta{
		Duration: b.Time.Sub(a.Time),
		StateItem: StateItem{
			RCU:                    b.CountersItem.RCU - a.CountersItem.RCU,
			WCU:                    b.CountersItem.WCU - a.CountersItem.WCU,
			ReadersPerSamplePeriod: b.CountersItem.ReadersPerSamplePeriod - a.CountersItem.ReadersPerSamplePeriod,
			WritersPerSamplePeriod: b.CountersItem.WritersPerSamplePeriod - a.CountersItem.WritersPerSamplePeriod,
		},
	}
}
