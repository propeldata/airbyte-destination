FROM golang:1.21-alpine as builder

WORKDIR /base

COPY . .

RUN apk --no-cache add make
RUN apk --no-cache add git

RUN make build

FROM alpine:3.19

WORKDIR /base

COPY --from=builder /base/propel-airbyte-destination-arm64 /base/

RUN chmod 755 /base/propel-airbyte-destination-arm64

LABEL io.airbyte.version=0.0.1
LABEL io.airbyte.name=airbyte/destination-propel

ENTRYPOINT ["/base/propel-airbyte-destination-arm64"]