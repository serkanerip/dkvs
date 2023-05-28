FROM golang:1.19 AS build

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS=linux go build -o dkvs cmd/dkvs/dkvs.go
RUN chmod +x /app/dkvs

FROM alpine:latest

WORKDIR /

COPY --from=build /app/dkvs /dkvs

CMD ["./dkvs"]