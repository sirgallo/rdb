package httpservice

import "net/http"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== HTTP Service


/*
	create a new service instance with passable options
	--> initialize the mux server and register route handlers on it, in this case the command route
		for sending operations to perform on the state machine
*/

func NewHTTPService(opts *HTTPServiceOpts) *HTTPService {
	mux := http.NewServeMux()

	httpService := &HTTPService{
		Mux: mux,
		Port: utils.NormalizePort(opts.Port),
		CurrentSystem: opts.CurrentSystem,
		RequestChannel: make(chan statemachine.StateMachineOperation, RequestChannelSize),
		ResponseChannel: make(chan statemachine.StateMachineResponse, ResponseChannelSize),
		ClientMappedResponseChannel: make(map[string]*chan statemachine.StateMachineResponse),
		Log: *clog.NewCustomLog(NAME),
	}

	httpService.RegisterCommandRoute()

	return httpService
}

/*
	Start HTTP Service
		separate go routines:
			1.) http server
				--> start the server to begin listening for client requests
			2.) handle response channel 
				--> for incoming respones, check the request id against the mapping of client response channels
					if the channel exists for the response, pass the response back to the route so it can be 
					returned to the client
*/

func (httpService *HTTPService) StartHTTPService() {
	go func() {
		httpService.Log.Info("http service starting up on port:", httpService.Port)

		srvErr := http.ListenAndServe(httpService.Port, httpService.Mux)
		if srvErr != nil { httpService.Log.Fatal("unable to start http service") }
	}()

	go func() {
		for {
			response :=<- httpService.ResponseChannel
			httpService.Mutex.Lock()
			clientChannel, ok := httpService.ClientMappedResponseChannel[response.RequestID]
			httpService.Mutex.Unlock()

			if ok {
				*clientChannel <- response
			} else { httpService.Log.Warn("no channel for resp associated with req uuid:", response.RequestID) }
		}
	}()
}