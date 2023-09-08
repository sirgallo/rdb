package relaytests

import "testing"

import "github.com/sirgallo/raft/pkg/relay"


func MockRelayService() *relay.RelayService[string] {
	return &relay.RelayService[string]{}
}

func TestRelay(t *testing.T) {
	
}