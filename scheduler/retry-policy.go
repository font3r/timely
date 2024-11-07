package scheduler

import (
	"errors"
	"time"
)

type StrategyType string

const (
	Constant    StrategyType = "constant"    // 100ms, 100ms, 100ms
	Linear      StrategyType = "linear"      // 100ms, 200ms, 300ms
	Exponential StrategyType = "exponential" // 100ms, 200ms, 400ms
)

type RetryPolicy struct {
	Strategy StrategyType // strategy
	Count    int          // maximum count of retries
	Interval string       // base interval for strategy
}

func NewRetryPolicy(strategyType StrategyType, count int, interval string) (RetryPolicy, error) {
	if strategyType != Constant && strategyType != Linear && strategyType != Exponential {
		return RetryPolicy{}, errors.New("invalid strategy type")
	}

	if count <= 0 {
		return RetryPolicy{}, errors.New("count must be greater than zero")
	}

	if interval == "" {
		return RetryPolicy{}, errors.New("missing interval")
	}

	_, err := time.ParseDuration(interval)
	if err != nil {
		return RetryPolicy{}, errors.New("invalid interval")
	}

	return RetryPolicy{
		Strategy: strategyType,
		Count:    count,
		Interval: interval,
	}, nil
}

func (rp RetryPolicy) GetNextExecutionTime(executionDate time.Time, attempt int) time.Time {
	d, _ := time.ParseDuration(rp.Interval)
	if attempt > rp.Count {
		return time.Time{}
	}

	// TODO: handle all strategies
	switch rp.Strategy {
	case Constant:
		{
			return executionDate.Add(d)
		}
	}

	return time.Now().Round(time.Second)
}
