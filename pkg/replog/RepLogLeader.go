package replog

import "context"
import "log"
import "sync"

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Service


/*
	Heartbeat:
		for all systems that have status alive, send an empty AppendEntryRPC to each
*/

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

	batchedRequests := rlService.truncatedRequests(requests)
	rlService.broadcastAppendEntryRPC(batchedRequests)
}

/*
	Replicate Logs:
		1.) append the new log to the replicated log on the leader,
			--> index is just the index of the last log + 1
			--> term is the current term on the leader
		2.) determine what systems are alive and prepare AppendEntryRPCs for each
			--> determine the next index of the system that the rpc is prepared for from the system object at NextIndex
		3.) on responses
			--> if the leader receives a success signal from the majority of the nodes in the cluster, 
				commit the logs to the state machine
*/

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

	batchedRequests := rlService.truncatedRequests(requests)
	successfulReplies := rlService.broadcastAppendEntryRPC(batchedRequests)
	if successfulReplies >= minSuccessfulReplies { rlService.CommitLogsLeader() }
}

/*
	Sync Logs:
		when a heartbeat is received and no logs have been applied to the replicated logs, sync up any systems 
		where the replicated log is incomplete or incorrect. 

		1.) determine what systems are behind on logs 
			--> status == Alive
			--> the NextIndex of the system is behind the commit index of the leader
				this means it is missing entries that have been safely commited to the state machine

		2.) prepare the AppendEntryRPCs for each system
*/

func (rlService *ReplicatedLogService[T]) SyncLogs() {
	requests := []ReplicatedLogRequest{}

	systemsBehind := utils.Filter[*system.System[T]](rlService.SystemsList, func(sys *system.System[T]) bool {
		if sys.Status == system.Alive && sys.NextIndex < rlService.CurrentSystem.CommitIndex { return true }
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

	batchedRequests := rlService.truncatedRequests(requests)
	rlService.broadcastAppendEntryRPC(batchedRequests)
}

/*
	Shared Broadcast RPC function:
		utilized by all three functions above
	
		for requests to be broadcasted:
			1.) send AppendEntryRPCs in parallel to each follower in the cluster
			2.) in each go routine handling each request, perform exponential backoff on failed requests until max retries
			3.) 
				if err: set the system status to dead and return
				if res:
					if success: 
						--> update total successful replies and update the next index of the system to the last log index of the reply
					else if failure and the reply has higher term than the leader: 
						--> update the state of the leader to follower, recognize a higher term and thus a more correct log
						--> signal that a higher term has been discovered and cancel all leftover requests
*/

func (rlService *ReplicatedLogService[T]) broadcastAppendEntryRPC(requestsPerHost []ReplicatedLogRequest) int {
	var appendEntryWG sync.WaitGroup

	successfulReplies := 0 
	higherTermDiscovered := make(chan bool, 1)

	for _, reqs := range requestsPerHost {
		appendEntryWG.Add(1)

		go func(req ReplicatedLogRequest) {
			defer appendEntryWG.Done()

			receivingHost := req.Host

			conn, connErr := rlService.ConnectionPool.GetConnection(receivingHost, rlService.Port)
			if connErr != nil { log.Fatalf("Failed to connect to %s: %v", receivingHost + rlService.Port, connErr) }

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