package main

import "log"
import "os"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/service"
import "github.com/sirgallo/raft/pkg/logger"
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
			HTTPService: 8080,
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
	
	select{}
}