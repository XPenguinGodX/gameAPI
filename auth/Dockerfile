FROM golang:1.25-alpine
WORKDIR /app

COPY . .

RUN go build -o auth-service ./auth/main.go

CMD ["./auth-service"]