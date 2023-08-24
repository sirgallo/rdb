package main

import "log"
import "net"
import "os"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


func main() {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { log.Fatal("unable to get hostname") }

	log.Println("hostname of system -->", hostname)

	lePort := 54321
	rlPort := 54322

	leListener, err := net.Listen("tcp", utils.NormalizePort(lePort))
	if err != nil { log.Fatalf("Failed to listen: %v", err) }

	rlListener, err := net.Listen("tcp", utils.NormalizePort(rlPort))
	if err != nil { log.Fatalf("Failed to listen: %v", err) }

	currentSystem := &system.System[string]{
		Host: hostname,
		CurrentTerm: 0,
		CommitIndex: 0,
		Replog: []*system.LogEntry[string]{},
	}

	systemsList := []*system.System[string]{
		{ Host: "hbsrv1", Status: system.Alive, NextIndex: -1 },
		{ Host: "hbsrv2", Status: system.Alive, NextIndex: -1 },
		{ Host: "hbsrv3", Status: system.Alive, NextIndex: -1 },
		{ Host: "hbsrv4", Status: system.Alive, NextIndex: -1 },
		{ Host: "hbsrv5", Status: system.Alive, NextIndex: -1 },
	}

	cpOpts := connpool.ConnectionPoolOpts{
		MaxConn: 10,
	}

	rlConnPool := connpool.NewConnectionPool(cpOpts)
	leConnPool := connpool.NewConnectionPool(cpOpts)

	rlOpts := &replog.ReplicatedLogOpts[string]{
		Port:           rlPort,
		ConnectionPool: rlConnPool,
		CurrentSystem:  currentSystem,
		SystemsList:    utils.Filter[*system.System[string]](systemsList, func(sys *system.System[string]) bool { 
			return sys.Host != hostname 
		}),
	}

	leOpts := &leaderelection.LeaderElectionOpts[string]{
		Port:           lePort,
		ConnectionPool: leConnPool,
		CurrentSystem:  currentSystem,
		SystemsList:    utils.Filter[*system.System[string]](systemsList, func(sys *system.System[string]) bool { 
			return sys.Host != hostname 
		}),
	}

	rlService := replog.NewReplicatedLogService[string](rlOpts)
	leService := leaderelection.NewLeaderElectionService[string](leOpts)

	go rlService.StartReplicatedLogService(&rlListener)
	go leService.StartLeaderElectionService(&leListener)

	go func () {
		for {
			<- rlService.LeaderAcknowledgedSignal
			leService.ResetTimeoutSignal <- true
		}
	}()
	
	select{}
}