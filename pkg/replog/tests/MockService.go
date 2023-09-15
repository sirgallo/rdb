package replogtests

import "os"
import "sync"

// import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/system"
// import "github.com/sirgallo/raft/pkg/wal"


const NAME = "Mock Replog Service"
var Log = clog.NewCustomLog(NAME)

func SetupMockReplogService() *replog.ReplicatedLogService[string] {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { Log.Fatal("unable to get hostname") }

	currentSystem := &system.System[string]{
		Host: hostname,
		CurrentTerm: 1,
		CommitIndex: 4,
		LastApplied: 4,
		Status: system.Ready,
		State: system.Follower,
		// WAL: wal,
	}

	systemsList := []*system.System[string]{
		{ Host: "1", NextIndex: 4, Status: system.Ready },
		{ Host: "2", NextIndex: 4, Status: system.Ready },
		{ Host: "3", NextIndex: 4, Status: system.Ready },
		{ Host: "4", NextIndex: 4, Status: system.Ready },
	}

	connpoolOpts := connpool.ConnectionPoolOpts{ MaxConn: 10 }
	rlConnPool := connpool.NewConnectionPool(connpoolOpts)

	sysMap := &sync.Map{} 
	for _, sys := range systemsList {
		sysMap.Store(sys.Host, sys)
	}

	rlOpts := &replog.ReplicatedLogOpts[string]{
		Port:	54322,
		ConnectionPool: rlConnPool,
		CurrentSystem: currentSystem,
		Systems: sysMap,
	}

	return replog.NewReplicatedLogService[string](rlOpts)
}