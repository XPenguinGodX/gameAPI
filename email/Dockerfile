FROM golang:1.25-alpine

WORKDIR /app
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o email .

CMD ["/app/email"]
