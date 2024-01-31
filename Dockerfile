FROM golang:1.21-alpine as builder

WORKDIR /base

COPY . .

RUN apk --no-cache add make git

RUN make build

FROM --platform=linux/amd64 amd64/alpine:3.19 AS runner

COPY --from=builder /base/propel-airbyte-destination ./
RUN chmod 755 ./propel-airbyte-destination

LABEL io.airbyte.version=0.0.1
LABEL io.airbyte.name=airbyte/destination-propel

ENTRYPOINT ["./propel-airbyte-destination"]