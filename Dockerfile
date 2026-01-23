FROM golang:1.25
LABEL authors="Francisco Perez"

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o server .

ENV API_PORT=8080
ENV DB_HOST=host.docker.internal
ENV SQL_PORT=3306
ENV SQL_ROOT=root
ENV SQL_PASSWORD=penguin
ENV DATABASE=RetroGameDatabase

EXPOSE 8080
CMD ["go", "run", "main.go" ]
