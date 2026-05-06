FROM golang:1.26 AS builder
WORKDIR /app
COPY . .
RUN go build tcpclient.go
CMD ["./tcpclient"]