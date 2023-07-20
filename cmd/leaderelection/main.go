package main

import "log"
import "net"
import "os"
import "strconv"

import "github.com/sirgallo/raft/pkg/leaderelection"
import "github.com/sirgallo/raft/pkg/shared"
import "github.com/sirgallo/raft/pkg/utils"


func main() {
	hostname, hostErr := os.Hostname()
	if hostErr != nil {
		log.Fatal("unable to get hostname")
	}

	log.Println("hostname of system -->", hostname)

	port := 54321

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	var currentTerm int64 = 0
	var lastLogIndex int64 = 0
	var lastLogTerm int64 = 0
	currentSystem := &shared.System{
		Host: hostname,
	}

	systemsList := []*shared.System{
		{Host: "lesrv1"},
		{Host: "lesrv2"},
		{Host: "lesrv3"},
		{Host: "lesrv4"},
		{Host: "lesrv5"},
	}

	leOpts := &leaderelection.LeaderElectionOpts{
		Port:          port,
		CurrentTerm:   &currentTerm,
		LastLogIndex:  &lastLogIndex,
		LastLogTerm:   &lastLogTerm,
		CurrentSystem: currentSystem,
		SystemsList:   utils.Filter[*shared.System](systemsList, func(sys *shared.System) bool { return sys.Host != hostname }),
	}

	leService := leaderelection.NewLeaderElectionService(leOpts)
	leService.StartLeaderElectionService(&listener)
}
