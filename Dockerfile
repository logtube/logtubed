FROM golang:1.13 AS builder
ENV CGO_ENABLED 0
WORKDIR /go/src/app
ADD . .
RUN go test -mod vendor -v
RUN go build -mod vendor -ldflags "-X main.Version=$(date -u +%Y-%m-%d_%H-%M-%S)" -o /logtubed

FROM guoyk/common-alpine:3.11
COPY --from=builder /logtubed /logtubed
CMD ["/logtubed"]
