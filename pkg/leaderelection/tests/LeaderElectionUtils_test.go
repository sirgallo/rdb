package leaderelectiontests

import "sync"
import "testing"

import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/system"


func TestInitializeTimeout(t *testing.T) {
	
}

func TestGetAliveSystemsAndMinVotes(t *testing.T) {
	systemsList := []*system.System[string]{
		{ Host: "1", NextIndex: 0, Status: system.Ready },
		{ Host: "2", NextIndex: 0, Status: system.Ready },
		{ Host: "3", NextIndex: 0, Status: system.Ready },
		{ Host: "4", NextIndex: 0, Status: system.Ready },
	}

	sysMap := &sync.Map{} 
	for _, sys := range systemsList {
		sysMap.Store(sys.Host, sys)
	}

	leService := &leaderelection.LeaderElectionService[string]{
		Systems: sysMap,
	}

	aliveSystems, minVotes := leService.GetAliveSystemsAndMinVotes()

	expectedAlive := 4
	expectedMinVotes := 3

	t.Logf("actual alive: %d, expected alive: %d\n", len(aliveSystems), expectedAlive)
	if len(aliveSystems) != expectedAlive {
		t.Errorf("actual total alive not equal to expected: actual(%d), expected(%d)\n", len(aliveSystems), expectedAlive)
	}

	t.Logf("actual votes: %d, expected votes: %d\n", minVotes, expectedMinVotes)
	if int(minVotes) != expectedMinVotes {
		t.Errorf("actual votes not equal to expected: actual(%d), expected(%d)\n", minVotes, expectedMinVotes)
	}
}