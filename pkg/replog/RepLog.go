package replog

import "context"
import "log"
import "net"
import "sync"
import "time"
import "google.golang.org/grpc"

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


func NewReplicatedLogService [T comparable](opts *ReplicatedLogOpts[T]) *ReplicatedLogService[T] {
	return &ReplicatedLogService[T]{
		Port: utils.NormalizePort(opts.Port),
		ConnectionPool: opts.ConnectionPool,
		CurrentSystem: opts.CurrentSystem,
		SystemsList: opts.SystemsList,
		AppendLogSignal: make(chan T),
		LeaderAcknowledgedSignal: make(chan bool),
	}
}

func (rlService *ReplicatedLogService[T]) StartReplicatedLogService(listener *net.Listener) {
	srv := grpc.NewServer()
	log.Println("replog gRPC server is listening on port:", rlService.Port)

	replogrpc.RegisterRepLogServiceServer(srv, rlService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { log.Fatalf("Failed to serve: %v", err) }
	}()

	for {
		select {
			case newCmd :=<- rlService.AppendLogSignal:
				if rlService.CurrentSystem.State == system.Leader {
					rlService.ReplicateLogs(newCmd)
				}
			case <- time.After(HeartbeatIntervalInMs * time.Millisecond):
				if rlService.CurrentSystem.State == system.Leader {
					log.Println("sending heartbeats...")
					rlService.Heartbeat()
				}
		}
	}
}

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
	lastLogIndex, lastLogTerm := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)

	newLog := &system.LogEntry[T]{
		Index: lastLogIndex + 1,
		Term: rlService.CurrentSystem.CurrentTerm,
		Command: cmd,
	}

	rlService.CurrentSystem.Replog = append(rlService.CurrentSystem.Replog, newLog)

	log.Println("rep log on leader -->", rlService.deferenceLogEntries())

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
				prevLogIndex = lastLogIndex
				prevLogTerm = lastLogTerm
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

				_, closeErr := rlService.ConnectionPool.CloseAllConnections(req.Host)
				if closeErr != nil { log.Println("close connection error", closeErr) }

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

func (rlService *ReplicatedLogService[T]) AppendEntryRPC(ctx context.Context, req *replogrpc.AppendEntry) (*replogrpc.AppendEntryResponse, error) {
	min := func (idx1, idx2 int64) int64 {
		if idx1 < idx2 { return idx1 }
		return idx2
	}
	
	sys := utils.Filter[*system.System[T]](rlService.SystemsList, func (sys *system.System[T]) bool { return sys.Host == req.LeaderId })[0]
	system.SetStatus[T](sys, true)

	var resp *replogrpc.AppendEntryResponse

	if req.Term < rlService.CurrentSystem.CurrentTerm {
		resp = &replogrpc.AppendEntryResponse{
			Term: rlService.CurrentSystem.CurrentTerm,
			Success: false,
		}
	}

	lastLogIndex, _ := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)

	if req.PrevLogIndex >= lastLogIndex || rlService.CurrentSystem.Replog[req.PrevLogIndex].Term != req.PrevLogTerm {
		resp = &replogrpc.AppendEntryResponse{
			Term: rlService.CurrentSystem.CurrentTerm,
			LatestLogIndex: lastLogIndex,
			Success: false,
		}
	}

	rlService.LeaderAcknowledgedSignal <- true

	for _, entry := range req.Entries {
		if rlService.checkIndex(entry.Index) {
			if rlService.CurrentSystem.Replog[entry.Index].Term != entry.Term { 
				rlService.CurrentSystem.Replog = rlService.CurrentSystem.Replog[entry.Index:] 
			}
		}

		cmd, decErr := utils.DecodeStringToStruct[T](entry.Command)
		if decErr != nil { log.Println("error on decode -->", decErr) }
		newLog := &system.LogEntry[T]{
			Index: entry.Index,
			Term: entry.Term,
			Command: *cmd,
		}

		rlService.CurrentSystem.Replog = append(rlService.CurrentSystem.Replog, newLog)
	}

	// log.Println("replog -->", rlService.deferenceLogEntries())

	if req.LeaderCommitIndex > rlService.CurrentSystem.CommitIndex {
		index := int64(len(rlService.CurrentSystem.Replog) - 1)
		rlService.CurrentSystem.CommitIndex = min(req.LeaderCommitIndex, index)
	}

	lastLogIndexAfterAppend, _ := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)

	resp = &replogrpc.AppendEntryResponse{
		Term: rlService.CurrentSystem.CurrentTerm,
		LatestLogIndex: lastLogIndexAfterAppend,
		Success: true,
	}

	return resp, nil
}


//========================================== helper methods


func (rlService *ReplicatedLogService[T]) checkIndex(index int64) bool {
	if index >= 0 && index < int64(len(rlService.CurrentSystem.Replog)) { return true }
	return false
}

func (rlService *ReplicatedLogService[T]) previousLogTerm(nextIndex int64) int64 {
	repLog := rlService.CurrentSystem.Replog[rlService.CurrentSystem.NextIndex]
	return repLog.Term
}

func (rlService *ReplicatedLogService[T]) deferenceLogEntries() []system.LogEntry[T] {
	var logEntries []system.LogEntry[T]
	for _, log := range rlService.CurrentSystem.Replog {
		logEntries = append(logEntries, *log)
	}

	return logEntries
}