FROM golang:1.21-alpine

WORKDIR /go/src/app
COPY . .

RUN apk add git

RUN go build -v .

RUN apk del git

CMD ["./brokerha"]
