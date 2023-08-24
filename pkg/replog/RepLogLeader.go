package replog

import "context"
import "log"
import "sync"

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


func (rlService *ReplicatedLogService[T]) Heartbeat() {
	sysHostPtr := &rlService.CurrentSystem.Host
	requests := []ReplicatedLogRequest{}

	for _, sys := range rlService.SystemsList {
		if sys.Status == system.Alive {
			var request ReplicatedLogRequest
			
			request.Host = sys.Host

			prevLogIndex, prevLogTerm := rlService.determinePreviousIndexAndTerm(sys)

			request.AppendEntry = &replogrpc.AppendEntry{
				Term: rlService.CurrentSystem.CurrentTerm,
				LeaderId: *sysHostPtr,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: prevLogTerm,
				Entries: nil,
				LeaderCommitIndex: rlService.CurrentSystem.CommitIndex,
			}

			requests = append(requests, request)
		}
	}

	batchedRequests := rlService.batchRequests(requests)
	rlService.broadcastAppendEntryRPC(batchedRequests)
}

func (rlService *ReplicatedLogService[T]) ReplicateLogs(cmd T) {
	lastLogIndex, _ := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)

	newLog := &system.LogEntry[T]{
		Index: lastLogIndex + 1,
		Term: rlService.CurrentSystem.CurrentTerm,
		Command: cmd,
	}

	rlService.CurrentSystem.Replog = append(rlService.CurrentSystem.Replog, newLog)
	
	requests := []ReplicatedLogRequest{}

	aliveSystems := utils.Filter[*system.System[T]](rlService.SystemsList, func (sys *system.System[T]) bool { 
		return sys.Status == system.Alive 
	})

	totalAliveSystems := len(aliveSystems) + 1
	minSuccessfulReplies := (totalAliveSystems / 2) + 1

	for _, sys := range aliveSystems {
		var request ReplicatedLogRequest
		request.Host = sys.Host

		prevLogIndex, prevLogTerm := rlService.determinePreviousIndexAndTerm(sys)
		appendEntry := rlService.prepareAppendEntryRPC(prevLogIndex, prevLogTerm)

		request.AppendEntry = appendEntry
		requests = append(requests, request)
	}

	batchedRequests := rlService.batchRequests(requests)
	successfulReplies := rlService.broadcastAppendEntryRPC(batchedRequests)
	if successfulReplies >= minSuccessfulReplies { rlService.CommitLogsLeader() }
}

func (rlService *ReplicatedLogService[T]) SyncLogs() {
	requests := []ReplicatedLogRequest{}

	systemsBehind := utils.Filter[*system.System[T]](rlService.SystemsList, func(sys *system.System[T]) bool {
		if sys.Status == system.Alive && sys.NextIndex != rlService.CurrentSystem.CommitIndex { return true }
		return false
	})

	for _, sys := range systemsBehind {
		var request ReplicatedLogRequest
		request.Host = sys.Host

		prevLogIndex, prevLogTerm := rlService.determinePreviousIndexAndTerm(sys)
		appendEntry := rlService.prepareAppendEntryRPC(prevLogIndex, prevLogTerm)

		request.AppendEntry = appendEntry
		requests = append(requests, request)
	}

	batchedRequests := rlService.batchRequests(requests)
	rlService.broadcastAppendEntryRPC(batchedRequests)
}

func (rlService *ReplicatedLogService[T]) broadcastAppendEntryRPC(requestChunksPerHost [][]ReplicatedLogRequest) int {
	var appendEntryWG sync.WaitGroup

	successfulReplies := 0 
	higherTermDiscovered := make(chan bool, 1)

	for _, reqs := range requestChunksPerHost {
		appendEntryWG.Add(1)

		go func(reqs []ReplicatedLogRequest) {
			defer appendEntryWG.Done()

			firstReq := reqs[0]
			receivingHost := firstReq.Host

			conn, connErr := rlService.ConnectionPool.GetConnection(receivingHost, rlService.Port)
			if connErr != nil { log.Fatalf("Failed to connect to %s: %v", receivingHost + rlService.Port, connErr) }

			client := replogrpc.NewRepLogServiceClient(conn)

			appendEntryRPC := func () (*replogrpc.AppendEntryResponse, error) {
				res, err := client.AppendEntryRPC(context.Background(), firstReq.AppendEntry)
				if err != nil { return utils.GetZero[*replogrpc.AppendEntryResponse](), err }
				return res, nil
			}

			maxRetries := 5
			expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInMilliseconds: 1 }
			expBackoff := utils.NewExponentialBackoffStrat[*replogrpc.AppendEntryResponse](expOpts)

			res, err := expBackoff.PerformBackoff(appendEntryRPC)

			sys := utils.Filter[*system.System[T]](rlService.SystemsList, func (sys *system.System[T]) bool { 
				return sys.Host == receivingHost
			})[0]

			if err != nil { 
				log.Printf("setting sytem %s to status dead", receivingHost)
				system.SetStatus[T](sys, false)
				return
			}

			if res.Success {
				successfulReplies += 1
				sys.NextIndex = res.LatestLogIndex
			} else {
				if res.Term > rlService.CurrentSystem.CurrentTerm { 
					rlService.CurrentSystem.State = system.Follower
					rlService.ConnectionPool.PutConnection(receivingHost, conn)
					
					higherTermDiscovered <- true
					return
				}
			}

			rlService.ConnectionPool.PutConnection(receivingHost, conn)
		}(reqs)
	}

	appendEntryWG.Wait()

	select {
		case <- higherTermDiscovered:
			return 0
		default:
			return successfulReplies
	}
}

func (rlService *ReplicatedLogService[T]) determinePreviousIndexAndTerm(sys *system.System[T]) (int64, int64) {
	var prevLogIndex, prevLogTerm int64
	
	if sys.NextIndex == -1 {
		prevLogIndex = 0
		if len(rlService.CurrentSystem.Replog) > 0 {
			prevLogTerm = rlService.CurrentSystem.Replog[0].Term
		} else {  prevLogTerm = 0 }
	} else {
		prevLogIndex = sys.NextIndex
		prevLogTerm = rlService.previousLogTerm(sys.NextIndex)
	}

	return prevLogIndex, prevLogTerm
}

func (rlService *ReplicatedLogService[T]) prepareAppendEntryRPC(prevLogIndex, prevLogTerm int64) *replogrpc.AppendEntry {
	sysHostPtr := &rlService.CurrentSystem.Host

	transform := func(logEntry *system.LogEntry[T]) *replogrpc.LogEntry {
		cmd, err := utils.EncodeStructToString[T](logEntry.Command)
		if err != nil { log.Println("error encoding log struct to string") }
		
		return &replogrpc.LogEntry{
			Index: logEntry.Index,
			Term: logEntry.Term,
			Command: cmd,
		}
	}

	entriesToSend := rlService.CurrentSystem.Replog[prevLogIndex:]
	entries := utils.Map[*system.LogEntry[T], *replogrpc.LogEntry](entriesToSend, transform)

	appendEntry := &replogrpc.AppendEntry{
		Term: rlService.CurrentSystem.CurrentTerm,
		LeaderId: *sysHostPtr,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: entries,
		LeaderCommitIndex: rlService.CurrentSystem.CommitIndex,
	}

	return appendEntry
}