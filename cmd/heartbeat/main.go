package main

import "log"
import "net"
import "os"
import "strconv"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/shared"
import "github.com/sirgallo/raft/pkg/utils"


func main() {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { log.Fatal("unable to get hostname") }

	log.Println("hostname of system -->", hostname)

	lePort := 54321
	rlPort := 54322

	leListener, err := net.Listen("tcp", ":" + strconv.Itoa(lePort))
	if err != nil { log.Fatalf("Failed to listen: %v", err) }

	rlListener, err := net.Listen("tcp", ":" + strconv.Itoa(rlPort))
	if err != nil { log.Fatalf("Failed to listen: %v", err) }

	currentSystem := &shared.System[string]{
		Host: hostname,
		CurrentTerm: 0,
		CommitIndex: 0,
		Replog: []*shared.LogEntry[string]{},
	}

	systemsList := []*shared.System[string]{
		{ Host: "hbsrv1" },
		{ Host: "hbsrv2" },
		{ Host: "hbsrv3" },
		{ Host: "hbsrv4" },
		{ Host: "hbsrv5" },
	}

	cpOpts := connpool.ConnectionPoolOpts{
		MinConn: 1,
		MaxConn: 10,
	}

	rlConnPool := connpool.NewConnectionPool(cpOpts)
	leConnPool := connpool.NewConnectionPool(cpOpts)

	rlOpts := &replog.ReplicatedLogOpts[string]{
		Port:           rlPort,
		ConnectionPool: rlConnPool,
		CurrentSystem:  currentSystem,
		SystemsList:    utils.Filter[*shared.System[string]](systemsList, func(sys *shared.System[string]) bool { 
			return sys.Host != hostname 
		}),
	}

	leOpts := &leaderelection.LeaderElectionOpts[string]{
		Port:           lePort,
		ConnectionPool: leConnPool,
		CurrentSystem:  currentSystem,
		SystemsList:    utils.Filter[*shared.System[string]](systemsList, func(sys *shared.System[string]) bool { 
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