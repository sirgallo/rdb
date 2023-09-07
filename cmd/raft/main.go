package main

import "log"
import "math/rand"
import "os"
import "time"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/service"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"
// import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/generated/keyvalstore"


type CommandEntry struct {
	Action string
	Data string
}


const NAME = "Main"
var Log = clog.NewCustomLog(NAME)


func main() {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { log.Fatal("unable to get hostname") }


	systemsList := []*system.System[keyvalstore.KeyValOp]{
		{ Host: "raftsrv1", NextIndex: 0 },
		{ Host: "raftsrv2", NextIndex: 0 },
		{ Host: "raftsrv3", NextIndex: 0 },
		{ Host: "raftsrv4", NextIndex: 0 },
		{ Host: "raftsrv5", NextIndex: 0 },
	}

	otherSystems := utils.Filter[*system.System[keyvalstore.KeyValOp]](systemsList, func(sys *system.System[keyvalstore.KeyValOp]) bool { 
		return sys.Host != hostname
	})

	raftOpts := service.RaftServiceOpts[keyvalstore.KeyValOp]{
		Protocol: "tcp",
		Ports: service.RaftPortOpts{
			LeaderElection: 54321,
			ReplicatedLog: 54322,
			Relay: 54323,
		},
		SystemsList: otherSystems,
		ConnPoolOpts: connpool.ConnectionPoolOpts{ MaxConn: 10 },
	}

	raft := service.NewRaftService[keyvalstore.KeyValOp](raftOpts)

	kvstore := keyvalstore.NewKeyValStore()

	go raft.StartRaftService()

	go func() {
		for {
			/*
			cmdEntry := &CommandEntry{
				Action: "insert",
				Data: "hi!",
			}
			*/

			cmdEntry := &keyvalstore.KeyValOp{
				Action: keyvalstore.SET,
				Data: keyvalstore.KeyValPair{
					Key: "hello",
					Value: "world",
				},
			}

			if raft.CurrentSystem.State == system.Leader {
				raft.ReplicatedLog.AppendLogSignal <- *cmdEntry
			} else { raft.Relay.RelayChannel <- *cmdEntry }
			
			
			randomNumber := rand.Intn(96) + 5
			time.Sleep(time.Duration(randomNumber) * time.Millisecond)
			// time.Sleep(100 * time.Microsecond)
		}
	}()

	go func() {
		for {
			logs :=<- raft.ReplicatedLog.LogCommitChannel
			completedLogs := []replog.LogCommitChannelEntry[keyvalstore.KeyValOp]{}
			for _, log := range logs {
				_, kvErr := kvstore.Ops(log.LogEntry.Command)
				if kvErr != nil { 
					log.Complete = false
				} else { log.Complete = true }

				completedLogs = append(completedLogs, log)
			}
			
			raft.ReplicatedLog.LogCommitChannel <- completedLogs
		}
	}()
	
	select{}
}