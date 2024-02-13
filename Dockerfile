FROM golang:1.21-alpine as builder

RUN apk --no-cache add make git

WORKDIR /base

COPY . .

RUN make build

FROM --platform=linux/amd64 amd64/alpine:3.19 AS runner

WORKDIR /base

COPY --from=builder /base/propel-airbyte-destination ./
RUN chmod 755 ./propel-airbyte-destination

LABEL io.airbyte.version=0.0.1
LABEL io.airbyte.name=airbyte/destination-propel

ENV AIRBYTE_ENTRYPOINT "./propel-airbyte-destination"
ENTRYPOINT ["./propel-airbyte-destination"]