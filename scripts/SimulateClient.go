package main

import "bytes"
import cryptoRand "crypto/rand"
import "crypto/tls"
import "encoding/base64"
import "encoding/json"
import "io"
// import mathRand "math/rand"
import "net/http"
import "os"
import "sync"
// import "time"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/utils"


const NAME = "Simulate Client"
var Log = clog.NewCustomLog(NAME)

const CONTENT_TYPE = "application/json"
const STRING_LENGTH = 30


func main() {
	genRandomString := func(length int) (string, error) {
		bytesNeeded := (length * 6) / 8 // base64 encoding uses 6 bits per character
		randomBytes := make([]byte, bytesNeeded)

		_, readErr := cryptoRand.Read(randomBytes)
		if readErr != nil { return utils.GetZero[string](), readErr }

		randomString := base64.RawURLEncoding.EncodeToString(randomBytes)
		return randomString[:length], nil
	}

	hostname, hostErr := os.Hostname()
	if hostErr != nil { Log.Fatal("unable to get hostname") }

	url := func () string { return "https://" + hostname + "/command" }()

	var clientWG sync.WaitGroup

	for range make([]int, 256) {
		clientWG.Add(1)

		go func() {
			defer clientWG.Done()
			
			client := &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: true,
					},
				},
			}

			/*
			sendRequestSignal := make(chan bool)
	
			
			go func() {
				for {
					sendRequestSignal <- true
		
					randomNumber := mathRand.Intn(5) + 1
					time.Sleep(time.Duration(randomNumber) * time.Second)
					//time.Sleep(time.Duration(500) * time.Microsecond)
				}
			}()
			*/
		
			for {
				var reqWG sync.WaitGroup

				reqWG.Add(1)
				
				go func() {
					defer reqWG.Done()

					randString, randErr := genRandomString(STRING_LENGTH)
					if randErr != nil { Log.Fatal("failed to generate random string:", randErr.Error()) }
					
					request := &statemachine.StateMachineOperation{
						Action: "insert",
						Payload: statemachine.StateMachineOpPayload{
							Collection: "test",
							Value: randString,
						},
					}
			
					requestJSON, encErr := json.Marshal(request)
					if encErr != nil { Log.Fatal("failed to encode request to json", encErr.Error()) }
					
					requestBuffer := bytes.NewBuffer(requestJSON)
					r, respErr := client.Post(url, CONTENT_TYPE, requestBuffer)
					if respErr != nil { Log.Fatal(respErr.Error()) }
			
					defer r.Body.Close()
					
					if r.StatusCode != 200 {
						responseBody, _ := io.ReadAll(r.Body)
						Log.Warn("status not 200", string(responseBody))
		
						return 
					}
		
					var response *statemachine.StateMachineResponse
			
					decodeErr := json.NewDecoder(r.Body).Decode(&response)
					if decodeErr != nil { Log.Fatal("failed to decode response", decodeErr.Error()) }
			
					Log.Debug("response:", response)
				}()

				reqWG.Wait()
			}

			// randomNumber := mathRand.Intn(5) + 1
			// time.Sleep(time.Duration(randomNumber) * time.Second)

		}()
	}

	clientWG.Wait()

	select{}
}