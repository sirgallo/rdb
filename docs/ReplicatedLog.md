# Replicated Log Module


## Overview

One of the major components of [Raft Consensus](https://raft.github.io/raft.pdf) is the leader election between systems within the cluster. Leader election is the method by which the systems reach consensus.


## Design

In this doc, the focus will be on the `AppendEntryRPC`, which is the remote procedure call used to request votes from other systems during an election phase.

The general layout of the request is:

```proto
message AppendEntry {
  int64 Term = 1;
  string LeaderId = 2;
  int64 PrevLogIndex = 3;
  int64 PrevLogTerm = 4;
  repeated LogEntry Entries = 5;
  int64 LeaderCommitIndex = 6;
}
```



## Algorithm

The basic algorithm is as follows:

```
Leader node in the cluster

  if no new logs are added to replicated log before heartbeat interval:
    broadcast heartbeat to all available nodes in the cluster:
      1. if success reply is received, maintain the node status as alive
      2. otherwise, label node as dead

      continue to next heartbeat or available log

  if new log is added to replicated log:
    for each system in the cluster:
      a. determine the next index that is to be applied to the system
      b. send an AppendEntryRPC to the node with the entries to be applied to it's replicated log
      c. if reply is failure and the term is higher from the reply, set the leader to follower
      d. set the follower next index to the latest index in the reply from the follower

    continue to next heartbeat or available log

Follower node in the cluster

  if request term is less than the current term of the system:
    return false with the term of the current system

  if the previous log being sent has index less than current log index of replicated log or the log at the index is not the term of the request:
    return false wit the current term and last log index

  otherwise:
    for each entry being applied in the request:
      if the index of the entry being applied is a different term than the current log:
        remove logs from the index forward
      
      append the new entry to the replicated log

    if the commit index of the leader is higher than the commit index of the current system:
      set the commit index of the current system to the minimum of the request commit index and the current commit index of the system

    return true with the current term and latest log index after appending the new logs
```


## Sources

[Replicated Log](../pkg/replog/RepLog.go)