package curator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockRetrySleeper struct {
	mock.Mock
}

func (s *MockRetrySleeper) SleepFor(time time.Duration) error {
	return s.Called(time).Error(0)
}

func TestRetryNTimes(t *testing.T) {
	d := 3 * time.Second
	p := NewRetryNTimes(3, d)
	s := &MockRetrySleeper{}

	assert.NotNil(t, p)

	s.On("SleepFor", d).Return(nil).Times(3)

	assert.True(t, p.AllowRetry(0, 0, s))
	assert.True(t, p.AllowRetry(1, 0, s))
	assert.True(t, p.AllowRetry(2, 0, s))
	assert.False(t, p.AllowRetry(3, 0, s))

	s.AssertExpectations(t)
}

func TestRetryOneTime(t *testing.T) {
	d := 3 * time.Second
	p := NewRetryOneTime(d)
	s := &MockRetrySleeper{}

	assert.NotNil(t, p)

	s.On("SleepFor", d).Return(nil).Once()

	assert.True(t, p.AllowRetry(0, 0, s))
	assert.False(t, p.AllowRetry(1, 0, s))

	s.AssertExpectations(t)
}

func TestExponentialBackoffRetry(t *testing.T) {
	d := 3 * time.Second
	p := NewExponentialBackoffRetry(d, 3, 9*time.Second)
	s := &MockRetrySleeper{}

	assert.NotNil(t, p)

	s.On("SleepFor", mock.AnythingOfType("Duration")).Return(nil).Times(3)

	assert.True(t, p.AllowRetry(0, 0, s))
	assert.True(t, p.AllowRetry(1, 0, s))
	assert.True(t, p.AllowRetry(2, 0, s))
	assert.False(t, p.AllowRetry(3, 0, s))

	assert.True(t, s.Calls[0].Arguments.Get(0).(time.Duration) < 1*d)
	assert.True(t, s.Calls[1].Arguments.Get(0).(time.Duration) < 2*d)
	assert.True(t, s.Calls[2].Arguments.Get(0).(time.Duration) < 4*d)

	s.AssertExpectations(t)
}

func TestRetryUntilElapsed(t *testing.T) {
	d := 3 * time.Second
	p := NewRetryUntilElapsed(3*d, d)
	s := &MockRetrySleeper{}

	assert.NotNil(t, p)

	s.On("SleepFor", d).Return(nil).Times(3)

	assert.True(t, p.AllowRetry(0, 0, s))
	assert.True(t, p.AllowRetry(0, d*1, s))
	assert.True(t, p.AllowRetry(0, d*2, s))
	assert.False(t, p.AllowRetry(0, d*3, s))

	s.AssertExpectations(t)
}
