package scheduler

import (
	"testing"
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
