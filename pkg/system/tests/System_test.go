package systemtests

import "os"
import "testing"

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/system"
// import "github.com/sirgallo/raft/pkg/wal"
import "github.com/sirgallo/raft/pkg/utils"


const NAME = "Mock System"
var Log = clog.NewCustomLog(NAME)

func SetupMockExistingSystem() *system.System[string] {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { Log.Fatal("unable to get hostname") }

	// wal, walErr := wal.NewWAL()
	// if walErr != nil { log.Fatal("unable to create or open WAL")  }
	
	replog := []*log.LogEntry[string]{
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
		// Replog: replog,
		// WAL: wal,
	}

	return currentSystem
}

func SetupMockNewSystem() *system.System[string] {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { Log.Fatal("unable to get hostname") }

	// wal, walErr := wal.NewWAL()
	// if walErr != nil { log.Fatal("unable to create or open WAL")  }

	currentSystem := &system.System[string]{
		Host: hostname,
		CurrentTerm: 0,
		CommitIndex: -1,
		LastApplied: -1,
		Status: system.Ready,
		State: system.Follower,
		// Replog: []*log.LogEntry[string]{},
		// WAL: wal,
	}

	return currentSystem
}

func TestLastLogIdxAndTermForExistingSystem(t *testing.T) {
	sys := SetupMockExistingSystem()
	lastLogIndex, lastLogTerm := system.DetermineLastLogIdxAndTerm[string](sys.Replog)

	expectedIndex := int64(4)
	expectedTerm := int64(1)

	t.Logf("actual index: %d, expected index: %d\n", lastLogIndex, expectedIndex)
	if lastLogIndex != expectedIndex {
		t.Errorf("actual index not equal to expected: actual(%d), expected(%d)\n", lastLogIndex, expectedIndex)
	}

	t.Logf("actual term: %d, expected term: %d\n", lastLogTerm, expectedTerm)
	if lastLogTerm != expectedTerm {
		t.Errorf("actual term not equal to expected: actual(%d), expected(%d)\n", lastLogTerm, expectedTerm)
	}
}

func TestLastLogIdxAndTermForNewSystem(t *testing.T) {
	sys := SetupMockNewSystem()
	lastLogIndex, lastLogTerm := system.DetermineLastLogIdxAndTerm[string](sys.Replog)

	expectedIndex := int64(-1)
	expectedTerm := int64(0)

	t.Logf("actual index: %d, expected index: %d\n", lastLogIndex, expectedIndex)
	if lastLogIndex != expectedIndex {
		t.Errorf("actual index not equal to expected: actual(%d), expected(%d)\n", lastLogIndex, expectedIndex)
	}

	t.Logf("actual term: %d, expected term: %d\n", lastLogTerm, expectedTerm)
	if lastLogTerm != expectedTerm {
		t.Errorf("actual term not equal to expected: actual(%d), expected(%d)\n", lastLogTerm, expectedTerm)
	}
}

func TestSetStatus(t *testing.T) {
	sys := SetupMockExistingSystem()

	t.Logf("set system to dead")
	
	sys.SetStatus(system.Dead)

	expectedStatus := system.Dead
	
	t.Logf("actual status for: %d, expected status for: %d\n", sys.Status, expectedStatus)
	if sys.Status != expectedStatus {
		t.Errorf("actual status for not equal to expected: actual(%d), expected(%d)\n", sys.Status, expectedStatus)
	}

	t.Logf("set system to ready")
	
	sys.SetStatus(system.Ready)

	expectedStatus = system.Ready
	
	t.Logf("actual status for: %d, expected status for: %d\n", sys.Status, expectedStatus)
	if sys.Status != expectedStatus {
		t.Errorf("actual status for not equal to expected: actual(%d), expected(%d)\n", sys.Status, expectedStatus)
	}

	t.Logf("set system to busy")
	
	sys.SetStatus(system.Busy)

	expectedStatus = system.Busy
	
	t.Logf("actual status for: %d, expected status for: %d\n", sys.Status, expectedStatus)
	if sys.Status != expectedStatus {
		t.Errorf("actual status for not equal to expected: actual(%d), expected(%d)\n", sys.Status, expectedStatus)
	}
}

func TestTransitionToCandidate(t *testing.T) {
	sys := SetupMockExistingSystem()
	sys.TransitionToCandidate()

	expectedState := system.Candidate
	expectedVotedFor := sys.Host
	expectedCurrentTerm := int64(2)

	t.Logf("actual state: %s, expected state: %s\n", sys.State, expectedState)
	if sys.State != expectedState {
		t.Errorf("actual state not equal to expected: actual(%s), expected(%s)\n", sys.State, expectedState)
	}

	t.Logf("actual voted for: %s, expected voted for: %s\n", sys.VotedFor, expectedVotedFor)
	if sys.VotedFor != expectedVotedFor {
		t.Errorf("actual voted for not equal to expected: actual(%s), expected(%s)\n", sys.VotedFor, expectedVotedFor)
	}

	t.Logf("actual term: %d, expected term: %d\n", sys.CurrentTerm, expectedCurrentTerm)
	if sys.CurrentTerm != expectedCurrentTerm {
		t.Errorf("actual term not equal to expected: actual(%d), expected(%d)\n", sys.CurrentTerm, expectedCurrentTerm)
	}
}

func TestTransitionToLeader(t *testing.T) {
	sys := SetupMockExistingSystem()
	sys.TransitionToLeader()

	expectedState := system.Leader

	t.Logf("actual state: %s, expected state: %s\n", sys.State, expectedState)
	if sys.State != expectedState {
		t.Errorf("actual state not equal to expected: actual(%s), expected(%s)\n", sys.State, expectedState)
	}
}

func TransitionToFollower(t *testing.T) {
	sys := SetupMockExistingSystem()

	t.Logf("transition to follower without voting and term change")

	opts := system.StateTransitionOpts{}
	sys.TransitionToFollower(opts)

	expectedState := system.Follower
	expectedVotedFor := utils.GetZero[string]()
	expectedCurrentTerm := int64(1)

	t.Logf("actual state: %s, expected state: %s\n", sys.State, expectedState)
	if sys.State != expectedState {
		t.Errorf("actual state not equal to expected: actual(%s), expected(%s)\n", sys.State, expectedState)
	}

	t.Logf("actual voted for: %s, expected voted for: %s\n", sys.VotedFor, expectedVotedFor)
	if sys.VotedFor != expectedVotedFor {
		t.Errorf("actual voted for not equal to expected: actual(%s), expected(%s)\n", sys.VotedFor, expectedVotedFor)
	}

	t.Logf("actual term: %d, expected term: %d\n", sys.CurrentTerm, expectedCurrentTerm)
	if sys.CurrentTerm != expectedCurrentTerm {
		t.Errorf("actual term not equal to expected: actual(%d), expected(%d)\n", sys.CurrentTerm, expectedCurrentTerm)
	}

	t.Logf("transition to follower term change")

	term := int64(2)
	opts = system.StateTransitionOpts{ CurrentTerm: &term }
	sys.TransitionToFollower(opts)

	expectedState = system.Follower
	expectedVotedFor = utils.GetZero[string]()
	expectedCurrentTerm = int64(2)

	t.Logf("actual state: %s, expected state: %s\n", sys.State, expectedState)
	if sys.State != expectedState {
		t.Errorf("actual state not equal to expected: actual(%s), expected(%s)\n", sys.State, expectedState)
	}

	t.Logf("actual voted for: %s, expected voted for: %s\n", sys.VotedFor, expectedVotedFor)
	if sys.VotedFor != expectedVotedFor {
		t.Errorf("actual voted for not equal to expected: actual(%s), expected(%s)\n", sys.VotedFor, expectedVotedFor)
	}

	t.Logf("actual term: %d, expected term: %d\n", sys.CurrentTerm, expectedCurrentTerm)
	if sys.CurrentTerm != expectedCurrentTerm {
		t.Errorf("actual term not equal to expected: actual(%d), expected(%d)\n", sys.CurrentTerm, expectedCurrentTerm)
	}

	t.Logf("transition to follower term change and update voted")

	term = int64(3)
	votedFor := "dummyleader"
	opts = system.StateTransitionOpts{ CurrentTerm: &term, VotedFor: &votedFor }
	sys.TransitionToFollower(opts)

	expectedState = system.Follower
	expectedVotedFor = votedFor
	expectedCurrentTerm = int64(3)

	t.Logf("actual state: %s, expected state: %s\n", sys.State, expectedState)
	if sys.State != expectedState {
		t.Errorf("actual state not equal to expected: actual(%s), expected(%s)\n", sys.State, expectedState)
	}

	t.Logf("actual voted for: %s, expected voted for: %s\n", sys.VotedFor, expectedVotedFor)
	if sys.VotedFor != expectedVotedFor {
		t.Errorf("actual voted for not equal to expected: actual(%s), expected(%s)\n", sys.VotedFor, expectedVotedFor)
	}

	t.Logf("actual term: %d, expected term: %d\n", sys.CurrentTerm, expectedCurrentTerm)
	if sys.CurrentTerm != expectedCurrentTerm {
		t.Errorf("actual term not equal to expected: actual(%d), expected(%d)\n", sys.CurrentTerm, expectedCurrentTerm)
	}
}