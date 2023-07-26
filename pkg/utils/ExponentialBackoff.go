package utils

import "errors"
import "math"
import "time"


const DefaultMaxRetries = -1 // let's use this to represent unlimited retries

type ExpBackoffOpts struct {
	TimeoutInMilliseconds int
	MaxRetries            *int // optional field, use a pointer
}

type ExponentialBackoffStrat [T comparable] struct {
	depth          int
	initialTimeout int
	currentTimeout int
	maxRetries     *int
}


func NewExponentialBackoffStrat [T comparable](opts ExpBackoffOpts) *ExponentialBackoffStrat[T] {
	maxRetries := DefaultMaxRetries
	if opts.MaxRetries != nil { maxRetries = *opts.MaxRetries }

	return &ExponentialBackoffStrat[T]{
		depth: 1, 
		initialTimeout: opts.TimeoutInMilliseconds,
		currentTimeout: opts.TimeoutInMilliseconds,
		maxRetries: &maxRetries,
	}
}

func (expStrat *ExponentialBackoffStrat[T]) PerformBackoff(operation func () (T, error)) (T, error) {
	if expStrat.depth > *expStrat.maxRetries && *expStrat.maxRetries != DefaultMaxRetries { 
		return GetZero[T](), errors.New("process reached max retries on exponential backoff") 
	}

	res, err := operation()
	
	if err != nil { 
		time.Sleep(time.Duration(expStrat.currentTimeout) * time.Millisecond)

		expStrat.currentTimeout = int(math.Pow(float64(2), float64(expStrat.depth - 1))) * expStrat.currentTimeout
		expStrat.depth++
		
		return expStrat.PerformBackoff(operation) 
	}

	return res, nil
}

func (expStrat *ExponentialBackoffStrat[T]) Reset() {
	expStrat.depth = 1
	expStrat.currentTimeout = expStrat.initialTimeout
}