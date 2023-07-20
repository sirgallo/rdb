package replog

import "context"
import "log"
import "net"
import "sync"
import "time"
import "google.golang.org/grpc"
import "google.golang.org/grpc/credentials/insecure"

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/shared"
import "github.com/sirgallo/raft/pkg/utils"


func NewReplicatedLogService [T comparable](opts *ReplicatedLogOpts) *ReplicatedLogService[T] {
	return &ReplicatedLogService[T]{
		Port: utils.NormalizePort(opts.Port),
		Replog: []*LogEntry[T]{},
	}
}

func (rlService *ReplicatedLogService[T]) StartReplicatedLogService(listener *net.Listener) {
	srv := grpc.NewServer()
	log.Println("gRPC server is listening on port:", rlService.Port)

	replogrpc.RegisterRepLogServiceServer(srv, rlService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { log.Fatalf("Failed to serve: %v", err) }
	}()
}

func (rlService *ReplicatedLogService[T]) Heartbeat() {
	sysHostPtr := &rlService.CurrentSystem.Host
	request := &replogrpc.AppendEntry{
		Term: *rlService.CurrentTerm,
		LeaderId: *sysHostPtr,
		PrevLogIndex: rlService.PrevLogIndex,
		Entries: nil,
		LeaderCommitIndex: *rlService.CommitIndex,
	}

	rlService.sendAppendEntryRPC(request)
}

func (rlService *ReplicatedLogService[T]) ReplicateLogs() {
	request := &replogrpc.AppendEntry{
		
	}

	rlService.sendAppendEntryRPC(request)
}

func (rlService *ReplicatedLogService[T]) sendAppendEntryRPC(request *replogrpc.AppendEntry) {
	var appendEntryWG sync.WaitGroup

	for _, sys := range rlService.SystemsList {
		appendEntryWG.Add(1)

		go func(sys *shared.System) {
			defer appendEntryWG.Done()

			conn, connErr := grpc.Dial(sys.Host + rlService.Port, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if connErr != nil { log.Fatalf("Failed to connect to %s: %v", sys.Host + rlService.Port, connErr) }

			client := replogrpc.NewRepLogServiceClient(conn)

			stream, strErr := client.AppendEntryRPC(context.Background())
			if strErr != nil { log.Println("failed to instantiate stream for appendEntryRPC -->", strErr) }

			sendErr := stream.SendMsg(request)
			if sendErr != nil { log.Printf("send error on %s: %s", rlService.CurrentSystem.Host, sendErr) }
		
			conn.Close()
		}(sys)
	}

	appendEntryWG.Wait()
}

func (rlService *ReplicatedLogService[T]) AppendEntriesRPC(stream replogrpc.RepLogService_AppendEntryRPCServer) error {
	for {
		_, reqErr := stream.Recv()
		if reqErr != nil { return reqErr }

		res := &replogrpc.AppendEntryResponse{
			Success: true,
		}

		respErr := stream.Send(res)
		if respErr != nil { return respErr }
	}
}