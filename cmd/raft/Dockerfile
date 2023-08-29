FROM golang:1.20

WORKDIR /usr/src/raft

COPY pkg ./pkg
COPY cmd ./cmd
COPY go.mod \
  go.sum ./

RUN go build -o raftsrv ./cmd/raft/main.go 

EXPOSE 54321
EXPOSE 54322

CMD ["./raftsrv"]