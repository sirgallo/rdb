FROM golang:1.20 as dependencies

FROM dependencies 

RUN apt-get update && apt-get install -y iputils-ping

ENV HOME /home

WORKDIR $HOME/raft

COPY pkg ./pkg
COPY cmd ./cmd
COPY go.mod \
  go.sum ./

RUN go build -o raftsrv ./cmd/raft/main.go 

EXPOSE 8080
EXPOSE 54321
EXPOSE 54322
EXPOSE 54323

CMD ["./raftsrv"]