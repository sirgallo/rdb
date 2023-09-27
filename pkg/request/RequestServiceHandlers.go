package request

import "encoding/json"
import "errors"
import "io"
import "net/http"
import "net/url"

import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"
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

func (reqService *RequestService) RegisterCommandRoute() {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost { 
			if reqService.CurrentSystem.State == system.Leader {
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

				clientResponseChannel := make(chan *statemachine.StateMachineResponse)
				reqService.ClientMappedResponseChannels.Store(hash, clientResponseChannel)

				requestData.RequestID = hash

				reqService.RequestChannel <- requestData
				responseData :=<- clientResponseChannel

				reqService.ClientMappedResponseChannels.Delete(hash)

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
			} else {
				redirectRequest := func() (bool, error) {
					if reqService.CurrentSystem.CurrentLeader != utils.GetZero[string]() {
						location := func () string { 
							return "http://" + reqService.CurrentSystem.CurrentLeader + reqService.Port + CommandRoute
						}()

						parsedURL, parseErr := url.Parse(location)
						if parseErr != nil { return false, parseErr }

						newReq := &http.Request{
							Method: r.Method,
							URL: parsedURL,
							Header: r.Header.Clone(), 
							Body:   r.Body,
						}

						client := &http.Client{}
						resp, postErr := client.Do(newReq)
						if postErr != nil { return false, postErr }
						
						defer resp.Body.Close()

						responseBody, readErr := io.ReadAll(resp.Body)
						if readErr != nil { return false, readErr }

						w.Header().Set("Content-Type", "application/json")
						w.Write(responseBody)

						return true, nil
					} else { return false, errors.New("current leader is not set for follower, aborting redirect") }
				}

				maxRetries := 5
				expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInMilliseconds: 50 }
				expBackoff := utils.NewExponentialBackoffStrat[bool](expOpts)

				_, redirectErr := expBackoff.PerformBackoff(redirectRequest)
				if redirectErr != nil { 
					http.Error(w, "current leader not found", http.StatusInternalServerError)
					return
				}
			}
		} else { http.Error(w, "method not allowed", http.StatusMethodNotAllowed) }
	}

	reqService.Mux.HandleFunc(CommandRoute, handler)
}