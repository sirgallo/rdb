package httpservice

import "context"
import "encoding/json"
import "net/http"

import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Snapshot Service Handlers


/*
	Register Command Route
		path: /command
		method: POST
		
		request body: 
			{
				action: "string",
				payload: {
					collection: "string",
					value: "string"
				}
			}
		
		response body:
			{
				collection: "string",
				key: "string" | nil,
				value: "string" | nil
			}

	ingest requests and pass from the HTTP Service to the replicated log service if leader, 
	or the relay service if a follower. 
		1.) append a both a unique identifier for the request as well as the current node that the request was sent to.
		2.) a channel for the request to be returned is created and mapped to the request id in the mapping of response channels
		2.) A context with timeout is initialized and the route either receives the response back and returns to the client,
			or the timeout is exceeded and failure is retuned to the client
*/

func (httpService *HTTPService) RegisterCommandRoute() {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost { 
			var requestData *statemachine.StateMachineOperation

			decodeErr := json.NewDecoder(r.Body).Decode(&requestData)
			if decodeErr != nil {
				http.Error(w, "failed to parse JSON request body", http.StatusBadRequest)
				return
			}

			hash, hashErr := utils.GenerateRandomSHA256Hash()
			if hashErr != nil {
				http.Error(w, "error producing hash for request id", http.StatusBadRequest)
				return
			}

			requestData.RequestID = hash
			requestData.RequestOrigin = httpService.CurrentSystem.Host

			httpService.Mutex.Lock()
			clientResponseChannel := make(chan statemachine.StateMachineResponse)
			httpService.ClientMappedResponseChannel[requestData.RequestID] = &clientResponseChannel
			httpService.Mutex.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), HTTPTimeout)
			defer cancel()

			select {
				case <- ctx.Done():
					delete(httpService.ClientMappedResponseChannel, requestData.RequestID)
					
					http.Error(w, "request timed out", http.StatusGatewayTimeout)
					return
				default:
					httpService.RequestChannel <- *requestData
					responseData :=<- clientResponseChannel

					delete(httpService.ClientMappedResponseChannel, requestData.RequestID)

					response := &statemachine.StateMachineResponse{
						Collection: responseData.Collection,
						Key: responseData.Key,
						Value: responseData.Value,
					}

					responseJSON, encErr := json.Marshal(response)
					if encErr != nil {
						http.Error(w, "Failed to encode JSON response", http.StatusInternalServerError)
						return
					}

					w.Header().Set("Content-Type", "application/json")
					w.Write(responseJSON)
			}
		} else { http.Error(w, "method not allowed", http.StatusMethodNotAllowed) }
	}

	httpService.Mux.HandleFunc(CommandRoute, handler)
}