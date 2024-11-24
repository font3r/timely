package scheduler

import (
	"errors"
	"fmt"
	"time"
)

type StrategyType string

const (
	Constant    StrategyType = "constant"    // 100ms, 100ms, 100ms, 100ms, 100ms
	Linear      StrategyType = "linear"      // 100ms, 200ms, 300ms, 400ms, 500ms
	Exponential StrategyType = "exponential" // 100ms, 200ms, 400ms, 800ms, 16000ms
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
	if attempt > rp.Count || attempt <= 0 {
		return time.Time{}
	}

	switch rp.Strategy {
	case Constant:
		{
			return executionDate.Add(d).Round(time.Second)
		}
	case Linear:
		{
			return executionDate.Add(time.Duration(d.Nanoseconds() * int64(attempt))).
				Round(time.Second)
		}
	default:
		// TODO: handle all strategies
		panic(fmt.Errorf("TODO: handle all strategies, missing %v", rp.Strategy))
	}
}
