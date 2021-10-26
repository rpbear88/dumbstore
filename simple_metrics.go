package main

import "fmt"
import "math"
import "time"

type IoMetrics struct {
	interval int 					// Stat interval(in second)
	thp int64							// IO throughput(MB/s)
	latency *Latency			// Write latency(in microsecond)
}

// n: Metrics interval(in seconds)
func NewIoMetrics(n int) (*IoMetrics) {
  m := &IoMetrics{n, 0, &Latency{}}
	c := time.Tick(time.Duration(n) * time.Second)
	go m.Display(c)
	return m
}

func (m *IoMetrics) Update(size int64, latency int64) {
	m.thp += size
	m.latency.Update(latency)
}

func (m *IoMetrics) Reset() {
	m.thp = 0
	m.latency.Reset()
}

func (m *IoMetrics) Thp() int64 {
	return m.thp / int64(1024 * 1024 * m.interval)
}

func (m *IoMetrics) Display(c <- chan time.Time) {
	for _ = range c {
		fmt.Println("Thp: ", m.Thp(), "(MB/s) Latency(ms) avg:", m.latency.Avg(), " max:", m.latency.Max(), " min:", m.latency.Min())
		m.Reset()
	}
}

type Latency struct {
	total int64
	count int64
	max int64
	min int64
}

func NewLatency() *Latency {
	return &Latency{0, 0, 0, math.MaxInt64}
}

func (l *Latency) Update(latency int64) {
	l.total += latency
	l.count += 1
	if latency > l.max {
		l.max = latency
	}
	if latency < l.min {
		l.min = latency
	}
}

func (l *Latency) Avg() int64 {
	if l.count == 0 {
		return 0
	}
	return l.total / l.count
}

func (l *Latency) Max() int64 { return l.max }

func (l *Latency) Min() int64 { return l.min }

func (l *Latency) Reset() {
	l.total = 0
	l.count = 0
	l.max = 0
	l.min = math.MaxInt64
}