package scheduler

import (
	"testing"
	"time"
)

func TestNewRetryPolicy(t *testing.T) {
	tests := map[string]struct {
		strategy StrategyType
		count    int
		interval string

		expected  RetryPolicy
		expectErr string
	}{
		"invalid_strategy_type": {
			strategy:  "test",
			count:     5,
			interval:  "1ms",
			expectErr: "invalid strategy type",
		},
		"count_less_than_0": {
			strategy:  Constant,
			count:     0,
			interval:  "1ms",
			expectErr: "count must be greater than zero",
		},
		"missing_interval": {
			strategy:  Constant,
			count:     5,
			interval:  "",
			expectErr: "missing interval",
		},
		"invalid_interval": {
			strategy:  Constant,
			count:     5,
			interval:  "1xd",
			expectErr: "invalid interval",
		},
		"valid": {
			strategy: Constant,
			count:    5,
			interval: "1ms",
			expected: RetryPolicy{Count: 5, Interval: "1ms", Strategy: Constant},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			rp, err := NewRetryPolicy(test.strategy, test.count, test.interval)

			if test.expectErr != "" {
				if test.expectErr != err.Error() {
					t.Errorf("expect error %s, got %s", test.expectErr, err.Error())
				}
			} else {
				if rp != test.expected {
					t.Errorf("expect result %+v, got %+v", test.expected, rp)
				}
			}
		})
	}
}

func TestGetNextExecutionPolicyAttemptsExceeded(t *testing.T) {
	rp, _ := NewRetryPolicy(Constant, 5, "10s")

	nextExecAt := rp.GetNextExecutionTime(getStubDate(), 10)

	if nextExecAt != (time.Time{}) {
		t.Errorf("expect result %+v, got %+v", time.Time{}, nextExecAt)
	}
}

func TestGetNextExecutionPolicyAttemptsLessOrEqualToZero(t *testing.T) {
	rp, _ := NewRetryPolicy(Constant, 5, "10s")

	tests := map[string]struct {
		attempt int

		expected time.Time
	}{
		"attempt_equal_to_0": {
			attempt:  0,
			expected: time.Time{},
		},
		"attempt_less_than_0": {
			attempt:  -1,
			expected: time.Time{},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			nextExecAt := rp.GetNextExecutionTime(getStubDate(), 0)
			if nextExecAt != test.expected {
				t.Errorf("expect result %+v, got %+v", time.Time{}, nextExecAt)
			}
		})
	}
}

func TestGetNextExecutionPolicyWithConstantStrategy(t *testing.T) {
	rp, _ := NewRetryPolicy(Constant, 5, "10s")

	for i := 1; i <= rp.Count; i++ {
		expected := getStubDate().Add(time.Duration(10*i) * time.Second).Round(time.Second)
		nextExecAt := rp.GetNextExecutionTime(getStubDate().Add(time.Duration(10*(i-1))*time.Second), i)

		if nextExecAt != expected {
			t.Errorf("expect result %+v, got %+v", expected, nextExecAt)
		}
	}
}

func TestGetNextExecutionPolicyWithLinearStrategy(t *testing.T) {
	rp, _ := NewRetryPolicy(Linear, 5, "10s")

	for i := 1; i <= rp.Count; i++ {
		nextExec, expected := getStubDate(), getStubDate()

		for j := 1; j <= i; j++ {
			expected = expected.Add(time.Duration(10*j) * time.Second).Round(time.Second)
			nextExec = nextExec.Add(time.Duration(10*(j-1)) * time.Second).Round(time.Second)
		}

		nextExecAt := rp.GetNextExecutionTime(nextExec, i)

		if nextExecAt != expected {
			t.Errorf("expect result %+v, got %+v", expected, nextExecAt)
		}
	}
}
