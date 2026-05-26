FROM golang:1.24-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/trends-service ./cmd/trends-service

FROM alpine:3.20

RUN adduser -D -H appuser

USER appuser

COPY --from=build /out/trends-service /trends-service

EXPOSE 8080

ENTRYPOINT ["/trends-service"]
