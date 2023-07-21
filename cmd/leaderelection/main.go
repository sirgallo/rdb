package main

import "log"
import "net"
import "os"
import "strconv"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/shared"
import "github.com/sirgallo/raft/pkg/utils"


func main() {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { log.Fatal("unable to get hostname") }

	log.Println("hostname of system -->", hostname)

	port := 54321

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil { log.Fatalf("Failed to listen: %v", err) }
	
	currentSystem := &shared.System[string]{
		Host: hostname,
		CurrentTerm: 0,
		CommitIndex: 0,
		Replog: []*shared.LogEntry[string]{},
	}

	systemsList := []*shared.System[string]{
		{ Host: "lesrv1" },
		{ Host: "lesrv2" },
		{ Host: "lesrv3" },
		{ Host: "lesrv4" },
		{ Host: "lesrv5" },
	}

	cpOpts := connpool.ConnectionPoolOpts{
		MinConn: 1,
		MaxConn: 10,
	}

	leOpts := &leaderelection.LeaderElectionOpts[string]{
		Port:           port,
		ConnectionPool: connpool.NewConnectionPool(cpOpts),
		CurrentSystem:  currentSystem,
		SystemsList:    utils.Filter[*shared.System[string]](systemsList, func(sys *shared.System[string]) bool { 
			return sys.Host != hostname 
		}),
	}

	leService := leaderelection.NewLeaderElectionService(leOpts)
	leService.StartLeaderElectionService(&listener)
}
