.PHONY: build

BINARY=propel-airbyte-destination
VERSION=0.0.1

build: propel-airbyte-destination build-arm64

build-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o ${BINARY}-arm64

propel-airbyte-destination:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ${BINARY}

secrets:
	rm -rf secrets
	mkdir secrets
	echo '{"application_id": "$(APP_ID)", "application_secret": "$(SECRET)"}' > secrets/config.json

test:
	go test -v ./internal/...

test-e2e:
	go test -v ./e2e/...