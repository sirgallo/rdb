package main

import "log"
import "math/rand"
import "net"
import "os"
import "time"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


type CommandEntry struct {
	Action string
	Data string
}

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

	currentSystem := &system.System[CommandEntry]{
		Host: hostname,
		CurrentTerm: 0,
		CommitIndex: 0,
		LastApplied: 0,
		Replog: []*system.LogEntry[CommandEntry]{},
	}

	systemsList := []*system.System[CommandEntry]{
		{ Host: "rlsrv1", Status: system.Alive, NextIndex: -1 },
		{ Host: "rlsrv2", Status: system.Alive, NextIndex: -1 },
		{ Host: "rlsrv3", Status: system.Alive, NextIndex: -1 },
		{ Host: "rlsrv4", Status: system.Alive, NextIndex: -1 },
		{ Host: "rlsrv5", Status: system.Alive, NextIndex: -1 },
	}

	cpOpts := connpool.ConnectionPoolOpts{
		MaxConn: 10,
	}

	rlConnPool := connpool.NewConnectionPool(cpOpts)
	leConnPool := connpool.NewConnectionPool(cpOpts)

	rlOpts := &replog.ReplicatedLogOpts[CommandEntry]{
		Port:           rlPort,
		ConnectionPool: rlConnPool,
		CurrentSystem:  currentSystem,
		SystemsList:    utils.Filter[*system.System[CommandEntry]](systemsList, func(sys *system.System[CommandEntry]) bool { 
			return sys.Host != hostname 
		}),
	}

	leOpts := &leaderelection.LeaderElectionOpts[CommandEntry]{
		Port:           lePort,
		ConnectionPool: leConnPool,
		CurrentSystem:  currentSystem,
		SystemsList:    utils.Filter[*system.System[CommandEntry]](systemsList, func(sys *system.System[CommandEntry]) bool { 
			return sys.Host != hostname 
		}),
	}

	rlService := replog.NewReplicatedLogService[CommandEntry](rlOpts)
	leService := leaderelection.NewLeaderElectionService[CommandEntry](leOpts)

	go rlService.StartReplicatedLogService(&rlListener)
	go leService.StartLeaderElectionService(&leListener)

	go func () {
		for {
			<- rlService.LeaderAcknowledgedSignal
			leService.ResetTimeoutSignal <- true
		}
	}()

	go func () {
		for {
			cmdEntry := &CommandEntry{
				Action: "insert",
				Data: "hi!",
			}

			rlService.AppendLogSignal <- *cmdEntry
			
			randomNumber := rand.Intn(96) + 5
			time.Sleep(time.Duration(randomNumber) * time.Millisecond)
		}
	}()

	go func () {
		for {
			logs := <- rlService.LogCommitChannel
			completedLogs := []replog.LogCommitChannelEntry[CommandEntry]{}
			for _, log := range logs {
				log.Complete = true
				completedLogs = append(completedLogs, log)
			}
			
			rlService.LogCommitChannel <- completedLogs
		}
	}()
	
	select{}
}