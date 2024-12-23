.PHONY: build

BINARY=propel-airbyte-destination
VERSION=0.0.7

build: build-amd64 build-arm64

build-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o ${BINARY}

build-amd64:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ${BINARY}

secrets:
	rm -rf secrets
	mkdir secrets
	echo '{"application_id": "$(APP_ID)", "application_secret": "$(SECRET)"}' > secrets/config.json

push-docker:
	docker buildx build -t propeldata/airbyte-propel-destination:$(VERSION) -t propeldata/airbyte-propel-destination:latest --platform linux/amd64,linux/arm64 . --push --build-arg VERSION=$(VERSION)

test:
	go test -v ./internal/...

test-e2e:
	go test -v ./e2e/...