package main

import "log"
import "math/rand"
import "os"
import "time"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/service"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


const NAME = "Main"
var Log = clog.NewCustomLog(NAME)


func main() {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { log.Fatal("unable to get hostname") }

	systemsList := []*system.System{
		{ Host: "raftsrv1" },
		{ Host: "raftsrv2" },
		{ Host: "raftsrv3" },
		{ Host: "raftsrv4" },
		{ Host: "raftsrv5" },
	}

	sysFilter := func(sys *system.System) bool { return sys.Host != hostname }
	otherSystems := utils.Filter[*system.System](systemsList, sysFilter)

	raftOpts := service.RaftServiceOpts{
		Protocol: "tcp",
		Ports: service.RaftPortOpts{
			LeaderElection: 54321,
			ReplicatedLog: 54322,
			Relay: 54323,
			Snapshot: 54324,
		},
		SystemsList: otherSystems,
		ConnPoolOpts: connpool.ConnectionPoolOpts{ MaxConn: 10 },
	}

	raft := service.NewRaftService(raftOpts)

	go raft.StartRaftService()

	// simulate a client creating new commands to be applied to the state machine
	go func() {
		for {
			cmdEntry := &statemachine.StateMachineOperation{
				Action: statemachine.INSERT,
				Payload: statemachine.StateMachineOpPayload{
					Collection: "test",
					Value: "hello world",
				},
			}

			raft.CommandChannel <- *cmdEntry
			
			randomNumber := rand.Intn(96) + 5
			time.Sleep(time.Duration(randomNumber) * time.Millisecond)
			// time.Sleep(100 * time.Microsecond)
		}
	}()
	
	select{}
}