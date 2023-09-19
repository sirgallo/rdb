FROM golang:1.20 as dependencies

FROM dependencies 

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
EXPOSE 54324

CMD ["./raftsrv"]