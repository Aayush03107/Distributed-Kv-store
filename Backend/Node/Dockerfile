FROM golang:latest
WORKDIR /app
COPY go.mod go.sum ./
COPY set/ ./set
COPY Node/ ./Node
WORKDIR /app/Node
RUN go build -o Raft .
COPY Node/nodes*.txt .
CMD ["./Raft"]