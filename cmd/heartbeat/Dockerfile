FROM golang:1.20

WORKDIR /usr/src/heartbeat

COPY pkg ./pkg
COPY cmd ./cmd
COPY go.mod \
  go.sum ./

RUN go build -o hbsrv ./cmd/heartbeat/main.go 

EXPOSE 54321
EXPOSE 54322

CMD ["./hbsrv"]