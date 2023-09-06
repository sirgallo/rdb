package utils

import "errors"
import "math"
import "time"


//=========================================== Exponential Backoff Utils


type ExpBackoffOpts struct {
	TimeoutInMilliseconds int
	MaxRetries *int // optional field, use a pointer
}

type ExponentialBackoffStrat [T comparable] struct {
	depth int
	initialTimeout int
	currentTimeout int
	maxRetries *int
}


const DefaultMaxRetries = -1 // let's use this to represent unlimited retries


/*
	create a new exponential backoff strategy with passable options
*/

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

/*
	Exponential Backoff

	pass in a function that returns type T, which will be our operation
		if success:
			return the response
		if error:
			1.) sleep for the current timeout period
			2.) recalculate the timeout for the next backoff period using:
				
				new timeout = 2 ^ (depth - 1) * current timeout
			
			3.) step to next retry
		if current depth exceeds the max retries defined:
		 return max retries error to indicate the operation failed
*/

func (expStrat *ExponentialBackoffStrat[T]) PerformBackoff(operation func() (T, error)) (T, error) {
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

/*
	reset the exponential backoff to highest depth and initial timeout
*/

func (expStrat *ExponentialBackoffStrat[T]) Reset() {
	expStrat.depth = 1
	expStrat.currentTimeout = expStrat.initialTimeout
}