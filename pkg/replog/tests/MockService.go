package replogtests

import "log"
import "os"
import "sync"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/system"
// import "github.com/sirgallo/raft/pkg/wal"


func SetupMockReplogService() *replog.ReplicatedLogService[string] {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { log.Fatal("unable to get hostname") }

	// wal, walErr := wal.NewWAL()
	// if walErr != nil { log.Fatal("unable to create or open WAL")  }

	initLog := []*system.LogEntry[string]{
		{ Index: 0, Term: 1, Command: "dummy" },
		{ Index: 1, Term: 1, Command: "dummy" },
		{ Index: 2, Term: 1, Command: "dummy" },
		{ Index: 3, Term: 1, Command: "dummy" },
		{ Index: 4, Term: 1, Command: "dummy" },
	}

	currentSystem := &system.System[string]{
		Host: hostname,
		CurrentTerm: 1,
		CommitIndex: 4,
		LastApplied: 4,
		Status: system.Ready,
		State: system.Follower,
		Replog: initLog,
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