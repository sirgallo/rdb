package main

import "log"
import "math/rand"
import "os"
import "time"

import "github.com/sirgallo/raft/pkg/service"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


type CommandEntry struct {
	Action string
	Data string
}


const NAME = "Main"
var Log = clog.NewCustomLog(NAME)


func main() {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { log.Fatal("unable to get hostname") }

	systemsList := []*system.System[CommandEntry]{
		{ Host: "raftsrv1", NextIndex: 0 },
		{ Host: "raftsrv2", NextIndex: 0 },
		{ Host: "raftsrv3", NextIndex: 0 },
		{ Host: "raftsrv4", NextIndex: 0 },
		{ Host: "raftsrv5", NextIndex: 0 },
	}

	otherSystems := utils.Filter[*system.System[CommandEntry]](systemsList, func(sys *system.System[CommandEntry]) bool { 
		return sys.Host != hostname
	})

	raftOpts := service.RaftServiceOpts[CommandEntry]{
		SystemsList: otherSystems,
	}

	raft := service.NewRaftService[CommandEntry](raftOpts)

	go raft.StartRaftService()

	go func () {
		for {
			cmdEntry := &CommandEntry{
				Action: "insert",
				Data: "hi!",
			}

			raft.ReplicatedLog.AppendLogSignal <- *cmdEntry
			
			randomNumber := rand.Intn(96) + 5
			time.Sleep(time.Duration(randomNumber) * time.Millisecond)
		}
	}()

	go func () {
		for {
			logs := <- raft.ReplicatedLog.LogCommitChannel
			completedLogs := []replog.LogCommitChannelEntry[CommandEntry]{}
			for _, log := range logs {
				log.Complete = true
				completedLogs = append(completedLogs, log)
			}
			
			raft.ReplicatedLog.LogCommitChannel <- completedLogs
		}
	}()
	
	select{}
}