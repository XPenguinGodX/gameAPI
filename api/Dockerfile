FROM golang:1.25-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download



COPY api/ ./api/
COPY kafka/ ./kafka/

WORKDIR /app/api
RUN CGO_ENABLED=0 go build -o /app/server .

EXPOSE 8080
CMD ["/app/server"]