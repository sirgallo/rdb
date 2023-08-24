package connpool

import "sync"


type ConnectionPoolOpts struct {
	MaxConn int
}

type ConnectionPool struct {
	connections sync.Map
	maxConn int
}