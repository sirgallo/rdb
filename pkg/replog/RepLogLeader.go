package replog

import "context"
import "log"
import "sync"

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


func (rlService *ReplicatedLogService[T]) Heartbeat() {
	sysHostPtr := &rlService.CurrentSystem.Host
	lastLogIndex, lastLogTerm := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)

	requests := []ReplicatedLogRequest{}

	for _, sys := range rlService.SystemsList {
		if sys.Status == system.Alive {
			var request ReplicatedLogRequest
			
			request.Host = sys.Host

			var prevLogIndex, prevLogTerm int64
			
			if sys.NextIndex == -1 {
				prevLogIndex = lastLogIndex
				prevLogTerm = lastLogTerm
			} else {
				prevLogIndex = sys.NextIndex
				prevLogTerm = rlService.previousLogTerm(sys.NextIndex)
			}

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

	rlService.broadcastAppendEntryRPC(requests)
}

func (rlService *ReplicatedLogService[T]) ReplicateLogs(cmd T) {
	sysHostPtr := &rlService.CurrentSystem.Host
	lastLogIndex, _ := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)

	newLog := &system.LogEntry[T]{
		Index: lastLogIndex + 1,
		Term: rlService.CurrentSystem.CurrentTerm,
		Command: cmd,
	}

	rlService.CurrentSystem.Replog = append(rlService.CurrentSystem.Replog, newLog)

	// log.Println("rep log on leader -->", rlService.deferenceLogEntries())

	requests := []ReplicatedLogRequest{}
	
	transform := func(logEntry *system.LogEntry[T]) *replogrpc.LogEntry {
		cmd, err := utils.EncodeStructToString[T](logEntry.Command)
		if err != nil { log.Println("error encoding log struct to string") }
		
		return &replogrpc.LogEntry{
			Index: logEntry.Index,
			Term: logEntry.Term,
			Command: cmd,
		}
	}

	for _, sys := range rlService.SystemsList {
		if sys.Status == system.Alive {
			var request ReplicatedLogRequest
			request.Host = sys.Host

			var prevLogIndex, prevLogTerm int64
			
			if sys.NextIndex == -1 {
				prevLogIndex = 0
				log := rlService.CurrentSystem.Replog[0]
				prevLogTerm = log.Term
			} else {
				prevLogIndex = sys.NextIndex
				prevLogTerm = rlService.previousLogTerm(sys.NextIndex)
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

			request.AppendEntry = appendEntry

			requests = append(requests, request)
		}
	}

	rlService.broadcastAppendEntryRPC(requests)
}

func (rlService *ReplicatedLogService[T]) broadcastAppendEntryRPC(requests []ReplicatedLogRequest) {
	var appendEntryWG sync.WaitGroup

	for _, req := range requests {
		appendEntryWG.Add(1)

		go func(req ReplicatedLogRequest) {
			defer appendEntryWG.Done()

			conn, connErr := rlService.ConnectionPool.GetConnection(req.Host, rlService.Port)
			if connErr != nil { log.Fatalf("Failed to connect to %s: %v", req.Host + rlService.Port, connErr) }

			client := replogrpc.NewRepLogServiceClient(conn)
		
			appendEntryRPC := func () (*replogrpc.AppendEntryResponse, error) {
				res, err := client.AppendEntryRPC(context.Background(), req.AppendEntry)
				if err != nil { return utils.GetZero[*replogrpc.AppendEntryResponse](), err }
				return res, nil
			}

			maxRetries := 5
			expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInMilliseconds: 1 }
			expBackoff := utils.NewExponentialBackoffStrat[*replogrpc.AppendEntryResponse](expOpts)

			res, err := expBackoff.PerformBackoff(appendEntryRPC)
			
			sys := utils.Filter[*system.System[T]](rlService.SystemsList, func (sys *system.System[T]) bool { return sys.Host == req.Host })[0]

			if err != nil { 
				log.Printf("setting sytem %s to status dead", req.Host)
				system.SetStatus[T](sys, false)

				return
			}

			if res.Success { 
				// log.Printf("AppendEntryRPC success on system: %s\n", req.Host) 
			} else {
				if res.Term > rlService.CurrentSystem.CurrentTerm { rlService.CurrentSystem.State = system.Follower }
			}

			sys.NextIndex = res.LatestLogIndex

			rlService.ConnectionPool.PutConnection(req.Host, conn)
		}(req)
	}

	appendEntryWG.Wait()
}