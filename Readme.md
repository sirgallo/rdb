# rdb (written in Go)


## Overview

`rdb` is a replicated database, utilizing raft consensus as the underlying distributed consensus algorithm. The Database is organized into collections, where each collection is a logical grouping of objects. Collections, by default, are also indexed.

This implementation of raft consensus aims to be readable and understandable, as well as performant.

The Raft Service is separated into modules, all of which are meant to be able to operate separately (for testing):

1. Leader Election Module
2. Replicated Log Module
3. Relay Module
4. Snapshot Module
5. HTTP Module

To learn more about each, check out:

[Leader Election](./docs/LeaderElection.md) 
[Replicated Log](./docs/ReplicatedLog.md)
[Relay](./docs/RelayModule.md)
[Snapshot](./docs/Snapshot.md)
[HTTPModule](./docs/Client.md)


The protocol buffer schemas are as follows:

  1. RequestVoteRPC
  2. AppendEntryRPC 
  3. RelayRPC
  4. SnapshotRPC 
  
All schemas can be found under [proto](./proto).


All code has been documented to make reasoning and readability of the code more straightforward. Going through the modules will give more in depth explainations of the algorithm than the documentation will.


## Deployment

This implementation includes `docker-compose` configuration for running a cluster locally. The `compose` file will run a cluster of 5 raft nodes, as well as a forward facing `haproxy` instance as the loadbalancer to send requests to.

First, ensure docker engine and docker compose are installed on your system (for `macos`, this involves installing docker desktop). [Click Here](https://www.docker.com/products/docker-desktop/) to download the latest version of docker desktop.

The basic implementation to run the cluster and the associated docker resources are located under [cmd](./cmd)

Once docker desktop is installed, run the following to deploy locally (on `macos`):

  1. Make sure HOSTNAME is set in ~/.zshrc and registered in /etc/hosts

```bash
export HOSTNAME=hostname
source ~/.zshrc
```

once sourced, restart your terminal for the changes to take effect

```bash
sudo nano /etc/hosts
```

open the file and add:
```bash
127.0.0.1 <your-hostname>
```

this binds your hostname to localhost now so you can use your hostname as the address to send commands to

  2. Generate the self signed certs

Haproxy is served over https, so generate your self signed certs under the [certs](./certs/) folder in the root of the project. This can be done with

```bash
cd ./certs
openssl req -newkey rsa:2048 -new -x509 -days 365 -nodes -out $HOSTNAME.crt -keyout $HOSTNAME.key
cat $HOSTNAME.key $HOSTNAME.crt > $HOSTNAME.pem
```

docker-compose will then bind the certs from your local machine to the certs folder on the haproxy container. Your hostname will also be bound as the hostname of the container

  3. Run the startupDev.sh script

Again, this is located in the root of the project. Run the following:

```bash
chmod +x ./startupDev.sh
./startupDev.sh
```

This will build the services and then start them. 

At this point, you can begin interacting with the cluster by sending it commands to perform on the state machine. 


## Interacting with the Cluster

The statemachine expects a commands with the following structure:

  1. Request

```json
{
    "action": string,
    "payload": {
        "collection": string,
        "value": string
    }
}
```

  2. Response

```json
{
    "collection": string,
    "key": string,
    "value": string
}
```

The Request is a `POST` request, which will send the request object to:
```
https://<your-host>/command
```


Here are the available commands, using `curl` to send requests to the cluster:

  1. find

```bash
curl --location 'https://<your-host>/command' \
--header 'Content-Type: application/json' \
--data '{
    "action": "find",
    "payload": {
        "collection": "<your-collection>",
        "value": "<your-value"
    }
}'
```

  2. insert

```bash
curl --location 'https://<your-host>/command' \
--header 'Content-Type: application/json' \
--data '{
    "action": "insert",
    "payload": {
        "collection": "<your-collection>",
        "value": "<your-value"
    }
}'
```

  3. delete

```bash
curl --location 'https://<your-host>/command' \
--header 'Content-Type: application/json' \
--data '{
    "action": "delete",
    "payload": {
        "collection": "<your-collection>",
        "value": "<your-value"
    }
}'
```

  4. drop collection

```bash
curl --location 'https://<your-host>/command' \
--header 'Content-Type: application/json' \
--data '{
    "action": "drop collection",
    "payload": {
        "collection": "<your-collection>",
        "value": "<your-value"
    }
}'
```


## To come

The database is currently in its infancy, so many changes are coming in the pipeline:
  
  1. range queries
  2. value schemas and indexing on fields in the structs
  3. better unit tests