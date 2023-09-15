# Relay Module


## Overview

A third module is added to the raft module, which is the relay module. This is basically just a way for followers to forward new commands to the leader if the follower is requested. It is pretty straightforward, with the following steps:

  1. if follower receives command, pipe the command the relay channel of the relay module
  2. prepare a relayrpc request and send the command to the current leader
  3. if the request fails, add to the failed buffer to retry
  4. if the request is sent to another follower, have that follower forward the log to the leader
  5. otherwise success


## Sources

[Relay](../pkg/relay/RelayService.go)