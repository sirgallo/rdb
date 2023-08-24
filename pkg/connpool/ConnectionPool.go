package connpool

import "errors"

import "google.golang.org/grpc"
import "google.golang.org/grpc/connectivity"
import "google.golang.org/grpc/credentials/insecure"


//=========================================== Connection Pool


/*
	initialize the connection pool

	the purpose of the connection pool is to reuse connections once they have been made, minimizing overhead
	for reconnecting to a host every time an rpc is made

	the pool has the following structure:
		{
			[key: address/host]: Array<connections>
		}
*/

func NewConnectionPool(opts ConnectionPoolOpts) *ConnectionPool {
	return &ConnectionPool{
		maxConn: opts.MaxConn,
	}
}

/*
	Get Connection:
		1.) load connections for the particular host/address
		2.) if the address was loaded from the thread safe map:
			if the total connections in the map is greater than max connections specified:
				--> throw max connections error
			otherwise for each connection in the array of connections, if the connection is not null and
			the connection is ready for work, return the connection

		3.) if the address was not loaded, create a new grpc connection and store the new connection at
		the key associated with the address/host and return the new connection
*/

func (cp *ConnectionPool) GetConnection(addr string, port string) (*grpc.ClientConn, error) {
	connections, loaded := cp.connections.Load(addr)
	if loaded {
		if len(connections.([]*grpc.ClientConn)) >= cp.maxConn { return nil, errors.New("max connections reached") }

		for _, conn := range connections.([]*grpc.ClientConn) {
			if conn != nil && conn.GetState() == connectivity.Ready { return conn, nil }
		}
	}

	newConn, connErr := grpc.Dial(addr + port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if connErr != nil { return nil, connErr }

	emptyConns, loaded := cp.connections.LoadOrStore(addr, []*grpc.ClientConn{newConn})
	if loaded {
		connections := emptyConns.([]*grpc.ClientConn)
		cp.connections.Store(addr, append(connections, newConn))
	}
	
	return newConn, nil
}

/*
	Put Connection:
		1.) load connections for the particular host/address
		2.) if the address was loaded from the thread safe map:
			if the connection already exists in the map, return 
			otherwise, close the connection and return
*/

func (cp *ConnectionPool) PutConnection(addr string, connection *grpc.ClientConn) (bool, error) {
	connections, loaded := cp.connections.Load(addr)
	if loaded {
		for _, conn := range connections.([]*grpc.ClientConn) {
			if conn == connection { return true, nil }
		}
	}

	closeErr := connection.Close()
	if closeErr != nil { return false, closeErr }
	
	return false, nil
}