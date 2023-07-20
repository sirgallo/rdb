FROM golang:1.20

WORKDIR /usr/src/replicatedlog

COPY pkg ./pkg
COPY cmd ./cmd
COPY go.mod \
  go.sum ./

RUN go build -o rlsrv ./cmd/replicatedlog/main.go 

EXPOSE 54321

CMD ["./rlsrv"]