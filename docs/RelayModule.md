# Relay Module


## Overview

A third module is added to the raft module, which is the relay module. This is basically just a way for followers to forward new commands to the leader if the follower is requested. It is pretty straightforward, with the following algorithm:
```
  1. if follower receives command, pipe the command the relay channel of the relay module
  2. prepare a relayrpc request and send the command to the current leader
  3. if the request fails, add to the failed buffer to retry
  4. if the request is sent to another follower, have that follower forward the log to the leader
  5. wait for a response back from the leader
  6. if a response from operating on the state machine is returned before the timeout, return the response back to the follower so it can be forwarded back to the client that issued it
```


## Sources

[Relay](../pkg/relay/RelayService.go)