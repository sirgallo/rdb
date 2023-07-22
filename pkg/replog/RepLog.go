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
		AppendLogSignal: make(chan *system.LogEntry[T], 10000),
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
			case newLog :=<- rlService.AppendLogSignal:
				if rlService.CurrentSystem.State == system.Leader {
					rlService.CurrentSystem.Replog = append(rlService.CurrentSystem.Replog, newLog)
					rlService.ReplicateLogs(newLog)
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
	
	request := &replogrpc.AppendEntry{
		Term: rlService.CurrentSystem.CurrentTerm,
		LeaderId: *sysHostPtr,
		PrevLogIndex: lastLogIndex,
		PrevLogTerm: lastLogTerm,
		Entries: nil,
		LeaderCommitIndex: rlService.CurrentSystem.CommitIndex,
	}

	rlService.sendAppendEntryRPC(request)
}

func (rlService *ReplicatedLogService[T]) ReplicateLogs(*system.LogEntry[T]) {
	sysHostPtr := &rlService.CurrentSystem.Host
	lastLogIndex, lastLogTerm := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)
	
	request := &replogrpc.AppendEntry{
		Term: rlService.CurrentSystem.CurrentTerm,
		LeaderId: *sysHostPtr,
		PrevLogIndex: lastLogIndex,
		PrevLogTerm: lastLogTerm,
		Entries: nil,
		LeaderCommitIndex: rlService.CurrentSystem.CommitIndex,
	}

	rlService.sendAppendEntryRPC(request)
}

func (rlService *ReplicatedLogService[T]) sendAppendEntryRPC(request *replogrpc.AppendEntry) {
	var appendEntryWG sync.WaitGroup

	for _, sys := range rlService.SystemsList {
		if sys.Status == system.Alive {
			appendEntryWG.Add(1)

			go func(sys *system.System[T]) {
				defer appendEntryWG.Done()
	
				conn, connErr := rlService.ConnectionPool.GetConnection(sys.Host, rlService.Port)
				if connErr != nil { log.Fatalf("Failed to connect to %s: %v", sys.Host + rlService.Port, connErr) }
	
				client := replogrpc.NewRepLogServiceClient(conn)
	
				_, reqErr := client.AppendEntryRPC(context.Background(), request)
				if reqErr != nil { log.Println("failed AppendEntryRPC -->", reqErr) }
			
				appendEntryRPC := func () (*replogrpc.AppendEntryResponse, error) {
					res, err := client.AppendEntryRPC(context.Background(), request)
					if err != nil { return utils.GetZero[*replogrpc.AppendEntryResponse](), err }
					return res, nil
				}
	
				maxRetries := 5
				expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInMilliseconds: 1 }
				expBackoff := utils.NewExponentialBackoffStrat[*replogrpc.AppendEntryResponse](expOpts)
	
				res, err := expBackoff.PerformBackoff(appendEntryRPC)
				if err != nil { 
					log.Printf("setting sytem %s to status dead", sys.Host)
					
					system.SetStatus[T](sys, false)

					_, closeErr := rlService.ConnectionPool.CloseAllConnections(sys.Host)
					if closeErr != nil { log.Println("close connection error", closeErr) }

					return
				}

				if res.Success { log.Printf("AppendEntryRPC success on system: %s\n", sys.Host)}
	
				rlService.ConnectionPool.PutConnection(sys.Host, conn)
			}(sys)
		}
	}

	appendEntryWG.Wait()
}

func (rlService *ReplicatedLogService[T]) AppendEntryRPC(ctx context.Context, req *replogrpc.AppendEntry) (*replogrpc.AppendEntryResponse, error) {
	sys := utils.Filter[*system.System[T]](rlService.SystemsList, func (sys *system.System[T]) bool { 
		return sys.Host == req.LeaderId 
	})[0]
	system.SetStatus[T](sys, true)

	var resp *replogrpc.AppendEntryResponse

	if req.Term < rlService.CurrentSystem.CurrentTerm {
		resp = &replogrpc.AppendEntryResponse{
			Success: false,
		}
	}

	lastLogIndex, _ := system.DetermineLastLogIdxAndTerm[T](rlService.CurrentSystem.Replog)

	if req.PrevLogIndex >= lastLogIndex || rlService.CurrentSystem.Replog[req.PrevLogIndex].Term != req.PrevLogTerm {
		resp = &replogrpc.AppendEntryResponse{
			Success: false,
		}
	}

	rlService.LeaderAcknowledgedSignal <- true

	for _, entry := range req.Entries {
		if rlService.CurrentSystem.Replog[entry.Index].Term != entry.Term { 
			rlService.CurrentSystem.Replog = rlService.CurrentSystem.Replog[entry.Index:] 
		}

		newLog := &system.LogEntry[T]{
			Term: entry.Term,
		}

		rlService.CurrentSystem.Replog = append(rlService.CurrentSystem.Replog, newLog)
	}

	if req.LeaderCommitIndex > rlService.CurrentSystem.CommitIndex {
		index := int64(len(rlService.CurrentSystem.Replog) - 1)
		rlService.CurrentSystem.CommitIndex = min(req.LeaderCommitIndex, index)
	}

	resp = &replogrpc.AppendEntryResponse{
		Success: true,
	}

	return resp, nil
}

func min(a, b int64) int64 {
	if a < b { return a }
	return b
}