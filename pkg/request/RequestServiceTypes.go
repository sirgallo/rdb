package request

import "net/http"
import "sync"
import "time"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"


type RequestServiceOpts struct {
	Port int
	CurrentSystem *system.System
}

type RequestService struct {
	Mux *http.ServeMux
	Port string
	Mutex sync.Mutex

	CurrentSystem *system.System
	
	RequestChannel chan *statemachine.StateMachineOperation
	ResponseChannel chan *statemachine.StateMachineResponse
	ClientMappedResponseChannel map[string]chan *statemachine.StateMachineResponse

	Log clog.CustomLog
}


const NAME = "HTTP Service"
const CommandRoute = "/command"
const RequestChannelSize = 1000000
const ResponseChannelSize = 1000000
const HTTPTimeout = 2 * time.Second