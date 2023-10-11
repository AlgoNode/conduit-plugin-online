LDFLAGS += -X github.com/algorand/conduit/version.Hash=$(shell git log -n 1 --pretty="%H")
LDFLAGS += -X github.com/algorand/conduit/version.ShortHash=$(shell git log -n 1 --pretty="%h")
LDFLAGS += -X github.com/algorand/conduit/version.CompileTime=$(shell date -u +%Y-%m-%dT%H:%M:%S%z)
LDFLAGS += -X "github.com/algorand/conduit/version.ReleaseVersion=0.0"

conduit:
	go build -ldflags='${LDFLAGS}' -o ./cmd/conduit/conduit cmd/conduit/main.go
	./cmd/conduit/conduit -v

clean:
	rm -f cmd/conduit/conduit

reset:
	rm -f cmd/conduit/data/metadata.json

