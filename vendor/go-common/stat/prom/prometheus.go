package prom

import "github.com/prometheus/client_golang/prometheus"

// Prom struct info
type Prom struct {
	timer   *prometheus.HistogramVec
	counter *prometheus.CounterVec
	state   *prometheus.GaugeVec
}

// New creates a Prom instance.
func New() *Prom {
	return &Prom{}
}

// WithTimer sets timer.
func (p *Prom) WithTimer(name string, labels []string) *Prom {
	if p == nil || p.timer != nil {
		return p
	}
	p.timer = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    name,
			Help:    name,
			Buckets: prometheus.LinearBuckets(0, 10, 300),
		}, labels)
	prometheus.MustRegister(p.timer)
	return p
}

// WithCounter sets counter.
func (p *Prom) WithCounter(name string, labels []string) *Prom {
	if p == nil || p.counter != nil {
		return p
	}
	p.counter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: name,
			Help: name,
		}, labels)
	prometheus.MustRegister(p.counter)
	return p
}

// WithState sets state.
func (p *Prom) WithState(name string, labels []string) *Prom {
	if p == nil || p.state != nil {
		return p
	}
	p.state = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: name,
			Help: name,
		}, labels)
	prometheus.MustRegister(p.state)
	return p
}

// Timing log timing information (in milliseconds) without sampling
func (p *Prom) Timing(name string, time int64, extra ...string) {
	if p.timer != nil {
		var label = append([]string{name}, extra...)
		p.timer.WithLabelValues(label...).Observe(float64(time))
	}
}

// Incr increments one stat counter without sampling
func (p *Prom) Incr(name string, extra ...string) {
	if p.counter != nil {
		var label = append([]string{name}, extra...)
		p.counter.WithLabelValues(label...).Inc()
	}
}

// State set state
func (p *Prom) State(name string, v int64) {
	if p.state != nil {
		p.state.With(prometheus.Labels{"name": name}).Set(float64(v))
	}
}

// Add add count    v must > 0
func (p *Prom) Add(name string, v int64, extra ...string) {
	if p.counter != nil {
		var label = append([]string{name}, extra...)
		p.counter.WithLabelValues(label...).Add(float64(v))
	}
}
