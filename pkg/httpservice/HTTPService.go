package httpservice

import "net/http"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/utils"


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