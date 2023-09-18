package main

import "log"
import "math/rand"
import "os"
import "time"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/service"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"
import "github.com/sirgallo/raft/adjunct/keyvalstore"


const NAME = "Main"
var Log = clog.NewCustomLog(NAME)


func main() {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { log.Fatal("unable to get hostname") }

	systemsList := []*system.System[keyvalstore.KeyValOp]{
		{ Host: "raftsrv1" },
		{ Host: "raftsrv2" },
		{ Host: "raftsrv3" },
		{ Host: "raftsrv4" },
		{ Host: "raftsrv5" },
	}

	sysFilter := func(sys *system.System[keyvalstore.KeyValOp]) bool { return sys.Host != hostname }
	otherSystems := utils.Filter[*system.System[keyvalstore.KeyValOp]](systemsList, sysFilter)

	kvstore := keyvalstore.NewKeyValStore()

	raftOpts := service.RaftServiceOpts[keyvalstore.KeyValOp, keyvalstore.KeyValOps, keyvalstore.KeyValPair, keyvalstore.KeyValStore]{
		Protocol: "tcp",
		Ports: service.RaftPortOpts{
			LeaderElection: 54321,
			ReplicatedLog: 54322,
			Relay: 54323,
			Snapshot: 54324,
		},
		SystemsList: otherSystems,
		ConnPoolOpts: connpool.ConnectionPoolOpts{ MaxConn: 10 },
		StateMachine: kvstore,
		SnapshotHandler: keyvalstore.SnapshotKeyValStore,
		// SnapshotReplayer: ,
	}

	raft := service.NewRaftService[keyvalstore.KeyValOp, keyvalstore.KeyValOps, keyvalstore.KeyValPair, keyvalstore.KeyValStore](raftOpts)

	go raft.StartRaftService()

	// simulate a client creating new commands to be applied to the state machine
	go func() {
		for {
			cmdEntry := &keyvalstore.KeyValOp{
				Action: keyvalstore.SET,
				Data: keyvalstore.KeyValPair{
					Key: "hello",
					Value: "world",
				},
			}

			raft.CommandChannel <- *cmdEntry
			
			randomNumber := rand.Intn(96) + 5
			time.Sleep(time.Duration(randomNumber) * time.Millisecond)
			// time.Sleep(100 * time.Microsecond)
		}
	}()

	go func() {
		for {
			logToApply :=<- raft.StateMachineLogApplyChan
			_, kvErr := kvstore.Ops(logToApply.LogEntry.Command)
			raft.StateMachineLogAppliedChan <- kvErr
		}
	}()
	
	select{}
}