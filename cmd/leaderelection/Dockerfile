FROM golang:1.20

WORKDIR /usr/src/leaderelection

COPY pkg ./pkg
COPY cmd ./cmd
COPY go.mod \
  go.sum ./

RUN go build -o lesrv ./cmd/leaderelection/main.go 

EXPOSE 54321

CMD ["./lesrv"]