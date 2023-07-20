# Raft (written in Go)


## Overview

This implementation of raft consensus aims to be readable and understandable. Nodes in the cluster communicate using [Proto Buffers](./docs/ProtoBuffers.md). 

The Raft nodes are separated into two main modules, both of which are meant to be able to operate separately (for testing):

1. Leader Election Module
2. Replicated Log Module

To learn more, check out [Leader Election](./docs/LeaderElection.md) and [Replicated Log](./docs/ReplicatedLog.md).

All raft applications are in `./cmd`. For more information on running the applications, check out [CMD](./cmd/Readme.md).

Both the `RequestVoteRPC` and `AppendEntryRPC` proto buffer schemas can be found under [proto](./proto).