FROM golang:1.20

WORKDIR /usr/src/replog

COPY pkg ./pkg
COPY cmd ./cmd
COPY go.mod \
  go.sum ./

RUN go build -o rlsrv ./cmd/replog/main.go 

EXPOSE 54321
EXPOSE 54322

CMD ["./rlsrv"]