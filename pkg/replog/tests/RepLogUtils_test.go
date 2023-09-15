package replogtests

import "sync"
import "testing"

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/replog"
import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"


func TestDetermineBatchSize(t *testing.T) {
	t.Logf("stub")
}

func TestPrepareAppendEntryRPC(t *testing.T) {
	mockService := SetupMockReplogService()
	aliveSystems, _ := mockService.GetAliveSystemsAndMinSuccessResps()

	sys := aliveSystems[0]
	appendEntry, prepareErr := mockService.PrepareAppendEntryRPC(sys.NextIndex, false)
	if prepareErr != nil { t.Errorf("error on preparing append entry rpc entries") }

	entries := []*replogrpc.LogEntry{ 
		{ Index:4, Term:1,Command:"\"dummy\""},
	}
	
	expected := &replogrpc.AppendEntry{
		Term: 1,
		LeaderId: mockService.CurrentSystem.Host,
		PrevLogIndex: 3,
		PrevLogTerm: 1,
		Entries: entries,
		LeaderCommitIndex: 4,
	}

	t.Logf("appendEntry: %v, expected: %v", appendEntry, expected)

	if (
		appendEntry.Term != expected.Term ||
		appendEntry.LeaderId != expected.LeaderId ||
		appendEntry.PrevLogIndex != expected.PrevLogIndex ||
		appendEntry.PrevLogTerm != expected.PrevLogTerm ||
		appendEntry.Entries[0].Command != expected.Entries[0].Command ||
		appendEntry.LeaderCommitIndex != expected.LeaderCommitIndex) {
		t.Errorf("actual entry not equal to expected: actual(%v), expected(%v)\n", appendEntry, expected)
	}
}

func TestCheckIndex(t *testing.T) {
	testLog := []*log.LogEntry[string]{
		{Index: 0, Term: 1, Command: "dummy"},
		{Index: 1, Term: 1, Command: "dummy"},
		{Index: 2, Term: 1, Command: "dummy"},
		{Index: 3, Term: 1, Command: "dummy"},
		{Index: 4, Term: 1, Command: "dummy"},
	}

	/*
	rlService := &replog.ReplicatedLogService[string]{
		CurrentSystem: &system.System[string]{ Replog: testLog },
	}

	ok := rlService.CheckIndex(int64(5))
*/
	//expected := false

	/*
	t.Logf("actual index exists: %v, expected index exists: %v\n", ok, expected)
	if ok != expected {
		t.Errorf("actual index exists not equal to expected: actual(%v), expected(%v)\n", ok, expected)
	}
	*/
}

func TestGetAliveSystemsAndMinSuccessResps(t *testing.T) {
	systemsList := []*system.System[string]{
		{Host: "1", NextIndex: 0, Status: system.Ready},
		{Host: "2", NextIndex: 0, Status: system.Ready},
		{Host: "3", NextIndex: 0, Status: system.Ready},
		{Host: "4", NextIndex: 0, Status: system.Ready},
	}

	sysMap := &sync.Map{}
	for _, sys := range systemsList {
		sysMap.Store(sys.Host, sys)
	}

	rlService := &replog.ReplicatedLogService[string]{
		Systems: sysMap,
	}

	aliveSystems, minSuccess := rlService.GetAliveSystemsAndMinSuccessResps()

	expectedAlive := 4
	expectedMinVotes := 3

	t.Logf("actual alive: %d, expected alive: %d\n", len(aliveSystems), expectedAlive)
	if len(aliveSystems) != expectedAlive {
		t.Errorf("actual total alive not equal to expected: actual(%d), expected(%d)\n", len(aliveSystems), expectedAlive)
	}

	t.Logf("actual min success: %d, expected min success: %d\n", minSuccess, expectedMinVotes)
	if int(minSuccess) != expectedMinVotes {
		t.Errorf("actual min success not equal to expected: actual(%d), expected(%d)\n", minSuccess, expectedMinVotes)
	}
}
