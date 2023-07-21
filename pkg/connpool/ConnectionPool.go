package connpool

import "errors"

import "google.golang.org/grpc"
import "google.golang.org/grpc/credentials/insecure"


func NewConnectionPool(opts ConnectionPoolOpts) *ConnectionPool {
	return &ConnectionPool{
		minConn: opts.MinConn,
		maxConn: opts.MaxConn,
	}
}

func (cp *ConnectionPool) GetConnection(addr string, port string) (*grpc.ClientConn, error) {
	connections, loaded := cp.connections.Load(addr)
	if loaded {
		for _, conn := range connections.([]*grpc.ClientConn) {
			if conn != nil { return conn, nil }
		}

		if len(connections.([]*grpc.ClientConn)) >= cp.maxConn {
			return nil, errors.New("max connections reached")
		}
	}

	newConn, connErr := grpc.Dial(addr + port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if connErr != nil { return nil, connErr }

	loadedConns, loaded := cp.connections.LoadOrStore(addr, []*grpc.ClientConn{newConn})
	if loaded {
		// If another goroutine added a connection array in the meantime,
		// add the new connection to that array
		connections := loadedConns.([]*grpc.ClientConn)
		if len(connections) >= cp.maxConn {
			return nil, errors.New("max connections reached")
		}

		cp.connections.Store(addr, append(connections, newConn))
	}
	
	return newConn, nil
}

func (cp *ConnectionPool) PutConnection(addr string, connection *grpc.ClientConn) (bool, error) {
	connections, loaded := cp.connections.Load(addr)
	if loaded {
		for _, conn := range connections.([]*grpc.ClientConn) {
			if conn == connection {
				conn.ResetConnectBackoff()
				return true, nil
			}
		}

		closeErr := connection.Close()
		if closeErr != nil { return false, closeErr }
	}
	
	return false, nil
}