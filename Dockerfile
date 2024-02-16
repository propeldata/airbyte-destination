FROM golang:1.21-alpine as builder

WORKDIR /base

COPY . .

RUN apk --no-cache add make git

RUN if [ "$(uname -m)" = "x86_64" ]; then make build-amd64; else make build-arm64; fi

FROM alpine:3.19

WORKDIR /base

COPY --from=builder /base/propel-airbyte-destination /base/
RUN chmod 755 /base/propel-airbyte-destination

LABEL io.airbyte.version=0.0.1
LABEL io.airbyte.name=airbyte/destination-propel

ENV AIRBYTE_ENTRYPOINT "/base/propel-airbyte-destination"
ENTRYPOINT ["/base/propel-airbyte-destination"]