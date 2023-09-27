# rdb (written in Go)


## Overview

`rdb` is a replicated database, utilizing raft consensus as the underlying distributed consensus algorithm. The Database is organized into collections, where each collection is a logical grouping of objects. Collections, by default, are also indexed.

This implementation of raft consensus aims to be readable and understandable, as well as performant.

The Raft Service is separated into modules, all of which are meant to be able to operate separately (for testing):

1. Leader Election Module
2. Replicated Log Module
3. Snapshot Module
4. Request Module

To learn more about each, check out:

[Leader Election](./docs/LeaderElection.md) 

[Replicated Log](./docs/ReplicatedLog.md)

[Snapshot](./docs/Snapshot.md)

[RequestModule](./docs/Client.md)


The protocol buffer schemas are as follows:

  1. RequestVoteRPC
  2. AppendEntryRPC 
  3. SnapshotRPC 
  
All schemas can be found under [proto](./proto).


For more information regarding the replicated log and state machine, check out:

[WAL](./docs/WAL.md)

[StateMachine](./docs/StateMachine.md)


All code has been documented to make reasoning and readability more straightforward. Going through the modules will give more in depth explainations of the algorithm than the documentation will.


## Deployment

This implementation includes `docker-compose` configuration for running a cluster locally. The `compose` file will run a cluster of 5 raft nodes, as well as a forward facing `haproxy` instance as the loadbalancer to send requests to.

First, ensure `docker engine` and `docker compose` are installed on your system (for `macos`, this involves installing `docker desktop`). [Click Here](https://www.docker.com/products/docker-desktop/) to download the latest version of `docker desktop`.

The basic implementation to run the cluster and the associated `docker` resources are located under [cmd](./cmd)

Once `docker desktop` is installed, run the following to deploy locally (on `macos`):

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

`docker-compose` will then bind the certs from your local machine to the certs folder on the haproxy container. Your hostname will also be bound as the hostname of the container

  3. Run the startupDev.sh script

Again, this is located in the root of the project. Run the following:

```bash
chmod +x ./startupDev.sh
./startupDev.sh
```

This will build the services and then start them. 

At this point, you can begin interacting with the cluster by sending it commands to perform on the state machine. 

To stop the cluster, run the `./stopDev.sh` script:
```bash
chmod +x ./stopDev.sh
./stopDev.sh
```

Stopping the cluster will bring down all of the services, but the volumes for each raft node are bound to the host machine for persistence. By default, the docker-compose file will create the volumes under your `$HOME` directory. 

For the replicated log db file, the path is:
```bash
$HOME/<raft-node-name>/replog/replog.db
```

For the statemachine db file, the path is:
```bash
$HOME/<raft-node-name>/statemachine/statemachine.db
```

Snapshots are stored under the same directory as the statemachine db file, with the following file format:
```
statemachine_hash.gz
```

To interact with the db files on the command line, the `bbolt` command line tool can be installed. This can be used to inspect existing buckets, stats, and information regarding the db. To install, run:

```bash
go get go.etcd.io/bbolt@latest
```

then run:
```bash
bbolt -h
```

This will give basic information regarding available commands to run against the db file.

If you want to remove the db files and snapshots, simply run:
```bash
rm -rf $HOME/raftsrv*
```


## Interacting with the Cluster

The statemachine expects commands with the following structure:

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


## To Come

  1. better unit tests
  2. update state machine to include object schemas (using reflection)