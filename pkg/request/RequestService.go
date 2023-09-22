package request

import "net/http"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Request Service


/*
	create a new service instance with passable options
	--> initialize the mux server and register route handlers on it, in this case the command route
		for sending operations to perform on the state machine
*/

func NewRequestService(opts *RequestServiceOpts) *RequestService {
	mux := http.NewServeMux()

	reqService := &RequestService{
		Mux: mux,
		Port: utils.NormalizePort(opts.Port),
		CurrentSystem: opts.CurrentSystem,
		RequestChannel: make(chan *statemachine.StateMachineOperation, RequestChannelSize),
		ResponseChannel: make(chan *statemachine.StateMachineResponse, ResponseChannelSize),
		ClientMappedResponseChannel: make(map[string]chan *statemachine.StateMachineResponse),
		Log: *clog.NewCustomLog(NAME),
	}

	reqService.RegisterCommandRoute()

	return reqService
}

/*
	Start Request Service
		separate go routines:
			1.) http server
				--> start the server to begin listening for client requests
			2.) handle response channel 
				--> for incoming respones, check the request id against the mapping of client response channels
					if the channel exists for the response, pass the response back to the route so it can be 
					returned to the client
*/

func (reqService *RequestService) StartHTTPService() {
	go func() {
		reqService.Log.Info("http service starting up on port:", reqService.Port)

		srvErr := http.ListenAndServe(reqService.Port, reqService.Mux)
		if srvErr != nil { reqService.Log.Fatal("unable to start http service") }
	}()

	go func() {
		for response := range reqService.ResponseChannel {
			reqService.Mutex.Lock()
			clientChannel, ok := reqService.ClientMappedResponseChannel[response.RequestID]
			reqService.Mutex.Unlock()

			if ok {
				clientChannel <- response
			} else { reqService.Log.Warn("no channel for resp associated with req id:", response.RequestID) }
		}
	}()
}