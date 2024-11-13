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

type Sched struct {
	burstPeriod           time.Duration
	samplesPerBurstPeriod int

	done            chan struct{}
	persister       Persister
	refreshInterval time.Duration

	lock         sync.Mutex
	batchRCURate *rate.Limiter
	batchWCURate *rate.Limiter
	localRCU     uint64
	localWCU     uint64
	samples      []Snapshot
}

func New(p Persister, refreshInterval time.Duration) *Sched {

	res := &Sched{
		burstPeriod:           300 * time.Second,
		samplesPerBurstPeriod: int(math.Round(300_000.0 / float64(refreshInterval.Milliseconds()))),

		done:            make(chan struct{}, 1),
		persister:       p,
		refreshInterval: refreshInterval,

		batchWCURate: &rate.Limiter{},
		samples:      make([]Snapshot, 0),
	}
	go refreshLoop(res)
	return res
}

func (s *Sched) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.done == nil {
		return
	}
	s.done <- struct{}{}
	s.done = nil
}

func refreshLoop(s *Sched) {
	for {
		select {
		case <-s.done:
			return
		case <-time.After(s.refreshInterval):
			s.refresh()
		}
	}
}

func (s *Sched) UsedWCU(n uint32) {
	s.lock.Lock()
	s.localWCU += uint64(n)
	s.lock.Unlock()
}

// WantWCU tells whether the available burst capacity can accommodate the given number of
// capacity units. If used, the caller should call UsedWCU() with the used amount.
func (s *Sched) WantWCU(n uint32) bool {
	return !s.batchWCURate.AllowN(time.Now(), int(n))
}

func (s *Sched) refresh() {
	pt, ok := s.persister.GetProvisionedThroughput()
	if !ok {
		// try again later
		return
	}
	s.lock.Lock()
	incRCU := s.localRCU
	incWCU := s.localWCU
	s.localRCU = 0
	s.localWCU = 0
	s.lock.Unlock()

	si, ok := s.persister.UpdateCountersItem("ADD RCU :incrcu, WCU :incwcu, ReadersPerSamplePeriod :one, WritersPerSamplePeriod :one",
		map[string]uint64{
			":incrcu": incRCU,
			":incwcu": incWCU,
			":one":    1,
		})
	if !ok {
		// try again later
		s.lock.Lock()
		s.localRCU += incRCU
		s.localWCU += incWCU
		s.lock.Unlock()
		return
	}
	t := time.Now()
	s.lock.Lock()
	defer s.lock.Unlock()
	s.samples = append(s.samples, Snapshot{
		ProvisionedGauge: pt,
		CountersItem:     si,
		Time:             t,
	})
	metrics := s.updateTargets()
	s.persister.UpdateMetrics(metrics, pt)
}

func (s *Sched) updateTargets() Metrics {
	if len(s.samples) > s.samplesPerBurstPeriod {
		s.samples = s.samples[len(s.samples)-s.samplesPerBurstPeriod:]
	}
	intervals, metrics := RecalculateIntervalsAndMetrics(s.samples)
	if intervals.RCUInterval.Nanoseconds() == 0 {
		s.batchRCURate = &rate.Limiter{}
	} else {
		s.batchRCURate = rate.NewLimiter(rate.Every(intervals.RCUInterval), int(s.burstPeriod/intervals.RCUInterval)/metrics.Readers+1)
	}
	if intervals.WCUInterval.Nanoseconds() == 0 {
		s.batchWCURate = &rate.Limiter{}
	} else {
		s.batchWCURate = rate.NewLimiter(rate.Every(intervals.WCUInterval), int(s.burstPeriod/intervals.WCUInterval)/metrics.Writers+1)
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
