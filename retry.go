package curator

import (
	"math"
	"math/rand"
	"time"
)

// Abstraction for retry policies to sleep
type RetrySleeper interface {
	// Sleep for the given time
	SleepFor(time time.Duration) error
}

// Abstracts the policy to use when retrying connections
type RetryPolicy interface {
	// Called when an operation has failed for some reason.
	// This method should return true to make another attempt.
	AllowRetry(retryCount int, elapsedTime time.Duration, sleeper RetrySleeper) bool
}

type SleepingRetry struct {
	RetryPolicy

	N            int
	getSleepTime func(retryCount int, elapsedTime time.Duration) time.Duration
}

func (r *SleepingRetry) AllowRetry(retryCount int, elapsedTime time.Duration, sleeper RetrySleeper) bool {
	if retryCount < r.N {
		if err := sleeper.SleepFor(r.getSleepTime(retryCount, elapsedTime)); err != nil {
			return false
		}

		return true
	}

	return false
}

// Retry policy that retries a max number of times
type RetryNTimes struct {
	SleepingRetry
}

func NewRetryNTimes(n int, sleepBetweenRetries time.Duration) *RetryNTimes {
	return &RetryNTimes{
		SleepingRetry: SleepingRetry{
			N:            n,
			getSleepTime: func(retryCount int, elapsedTime time.Duration) time.Duration { return sleepBetweenRetries },
		},
	}
}

// A retry policy that retries only once
type RetryOneTime struct {
	RetryNTimes
}

func NewRetryOneTime(sleepBetweenRetry time.Duration) *RetryOneTime {
	return &RetryOneTime{
		*NewRetryNTimes(1, sleepBetweenRetry),
	}
}

const (
	MAX_RETRIES_LIMIT               = 29
	DEFAULT_MAX_SLEEP time.Duration = time.Duration(math.MaxInt64 * int64(time.Second))
)

// Retry policy that retries a set number of times with increasing sleep time between retries
type ExponentialBackoffRetry struct {
	SleepingRetry
}

func NewExponentialBackoffRetry(baseSleepTime time.Duration, maxRetries int, maxSleep time.Duration) *ExponentialBackoffRetry {
	if maxRetries > MAX_RETRIES_LIMIT {
		maxRetries = MAX_RETRIES_LIMIT
	}

	return &ExponentialBackoffRetry{
		SleepingRetry: SleepingRetry{
			N: maxRetries,
			getSleepTime: func(retryCount int, elapsedTime time.Duration) time.Duration {
				sleepTime := time.Duration(int64(baseSleepTime) * rand.Int63n(1<<uint(retryCount)))

				if sleepTime > maxSleep {
					sleepTime = maxSleep
				}

				return sleepTime
			},
		}}
}

// A retry policy that retries until a given amount of time elapses
type RetryUntilElapsed struct {
	SleepingRetry

	maxElapsedTime time.Duration
}

func NewRetryUntilElapsed(maxElapsedTime, sleepBetweenRetries time.Duration) *RetryUntilElapsed {
	return &RetryUntilElapsed{
		SleepingRetry: SleepingRetry{
			N:            math.MaxInt64,
			getSleepTime: func(retryCount int, elapsedTime time.Duration) time.Duration { return sleepBetweenRetries },
		},
		maxElapsedTime: maxElapsedTime,
	}
}

func (r *RetryUntilElapsed) AllowRetry(retryCount int, elapsedTime time.Duration, sleeper RetrySleeper) bool {
	return elapsedTime < r.maxElapsedTime && r.SleepingRetry.AllowRetry(retryCount, elapsedTime, sleeper)
}
