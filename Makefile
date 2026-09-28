.PHONY: all build test fmt fmt-check vet check clean

all: check build

build:
	go build -o agm ./cmd/agm

test:
	go test -race ./...

fmt:
	gofmt -w .

fmt-check:
	@files=$$(gofmt -l .) || exit $$?; if [ -n "$$files" ]; then echo "gofmt needed:"; echo "$$files"; exit 1; fi

vet:
	go vet ./...

# What CI runs on every push and pull request.
check: fmt-check vet test

clean:
	rm -f agm
