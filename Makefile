.PHONY: build

BINARY=propel-airbyte-destination
VERSION=0.0.1

build: propel-airbyte-destination

propel-airbyte-destination:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ${BINARY}

test:
	go test -v ./...