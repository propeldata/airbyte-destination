package connector

import (
	"fmt"
	"time"
)

// stateRefreshFunc is a function responsible for refreshing the item being watched for a state change.
type stateRefreshFunc[T any] func() (result *T, state string, err error)

type stateChangeOps[T any] struct {
	pending []string
	target  []string
	refresh stateRefreshFunc[T]
	timeout time.Duration
	delay   time.Duration
}

func waitForState[T any](ops stateChangeOps[T]) (*T, error) {
	start := time.Now()

	for {
		elapsed := time.Since(start)
		if elapsed.Milliseconds() > ops.timeout.Milliseconds() {
			return nil, fmt.Errorf("timeout waiting for state to change to %q", ops.target)
		}

		res, currentState, err := ops.refresh()
		if err != nil {
			return nil, err
		}

		for _, targetState := range ops.target {
			if currentState == targetState {
				return res, nil
			}
		}

		pendingFound := false

		for _, pendingState := range ops.pending {
			if currentState == pendingState {
				pendingFound = true
				break
			}
		}

		if !pendingFound {
			return nil, fmt.Errorf("received an unexpected state %s. Expected states are %q", currentState, ops.target)
		}

		time.Sleep(ops.delay)
	}
}
