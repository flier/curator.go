package curator

import (
	"sync"
	"time"

	"github.com/golang/glog"
)

// Mechanism for timing methods and recording counters
type TracerDriver interface {
	// Record the given trace event
	AddTime(name string, d time.Duration)

	// Add to a named counter
	AddCount(name string, increment int)
}

type Tracer interface {
	Commit()
}

type defaultTracerDriver struct {
	TracerDriver

	lock     sync.Mutex
	counters map[string]int
}

func newDefaultTracerDriver() *defaultTracerDriver {
	return &defaultTracerDriver{counters: make(map[string]int)}
}

func (d *defaultTracerDriver) AddTime(name string, time time.Duration) {
	if glog.V(3) {
		glog.V(3).Infof("Trace: %s - %s", name, time)
	}
}

func (d *defaultTracerDriver) AddCount(name string, increment int) {
	d.lock.Lock()

	value, _ := d.counters[name]

	value += increment

	d.counters[name] = value

	d.lock.Unlock()

	if glog.V(3) {
		glog.V(3).Infof("Counter %s: %d %d", name, value, increment)
	}
}

// Utility to time a method or portion of code
type timeTrace struct {
	name      string
	driver    TracerDriver
	startTime time.Time
}

// Create and start a timer
func newTimeTrace(name string, driver TracerDriver) *timeTrace {
	return &timeTrace{
		name:      name,
		driver:    driver,
		startTime: time.Now(),
	}
}

// Record the elapsed time
func (t *timeTrace) Commit() {
	t.driver.AddTime(t.name, time.Now().Sub(t.startTime))
}
