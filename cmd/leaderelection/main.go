package main

import "log"
import "net"
import "os"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


func main() {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { log.Fatal("unable to get hostname") }

	log.Println("hostname of system -->", hostname)

	port := 54321

	listener, err := net.Listen("tcp", utils.NormalizePort(port))
	if err != nil { log.Fatalf("Failed to listen: %v", err) }
	
	currentSystem := &system.System[string]{
		Host: hostname,
		CurrentTerm: 0,
		CommitIndex: 0,
		Replog: []*system.LogEntry[string]{},
	}

	systemsList := []*system.System[string]{
		{ Host: "lesrv1", Status: system.Alive, NextIndex: -1 },
		{ Host: "lesrv2", Status: system.Alive, NextIndex: -1 },
		{ Host: "lesrv3", Status: system.Alive, NextIndex: -1 },
		{ Host: "lesrv4", Status: system.Alive, NextIndex: -1 },
		{ Host: "lesrv5", Status: system.Alive, NextIndex: -1 },
	}

	cpOpts := connpool.ConnectionPoolOpts{
		MinConn: 1,
		MaxConn: 10,
	}

	leOpts := &leaderelection.LeaderElectionOpts[string]{
		Port:           port,
		ConnectionPool: connpool.NewConnectionPool(cpOpts),
		CurrentSystem:  currentSystem,
		SystemsList:    utils.Filter[*system.System[string]](systemsList, func(sys *system.System[string]) bool { 
			return sys.Host != hostname 
		}),
	}

	leService := leaderelection.NewLeaderElectionService(leOpts)
	leService.StartLeaderElectionService(&listener)
}
