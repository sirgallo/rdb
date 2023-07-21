package connpool

import "sync"


type ConnectionPoolOpts struct {
	MinConn int
	MaxConn int
}

type ConnectionPool struct {
	connections sync.Map
	minConn int
	maxConn int
}