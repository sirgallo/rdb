package httpservice

import "context"
import "encoding/json"
import "net/http"

import "github.com/sirgallo/raft/pkg/statemachine"


func (httpService *HTTPService) RegisterCommandRoute() {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost { 
			var requestData *statemachine.StateMachineOperation

			decodeErr := json.NewDecoder(r.Body).Decode(&requestData)
			if decodeErr != nil {
				http.Error(w, "failed to parse JSON request body", http.StatusBadRequest)
				return
			}

			requestData.RequestID = httpService.GenerateRequestUUID()
			requestData.RequestOrigin = httpService.CurrentSystem.Host

			clientResponseChannel := make(chan statemachine.StateMachineResponse)

			httpService.Mutex.Lock()
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