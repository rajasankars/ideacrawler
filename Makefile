GOPATHDIR=$(firstword $(subst :, ,${GOPATH}))
GOBIN=$(GOPATHDIR)/bin

default: build

clean:
	if [ -d build ]; then rm -rf build; fi

proto:
ifeq ($(GOPATHDIR),)
	@echo "No gopath"
	@exit 1
endif
	protoc -I $(PWD)/ $(PWD)/protofiles/ideacrawler.proto --go_out=plugins=grpc:$(PWD)/

protopy:
	python -m grpc_tools.protoc -I $(PWD)/protofiles --python_out=$(PWD)/protofiles --grpc_python_out=$(PWD)/protofiles $(PWD)/protofiles/ideacrawler.proto

build: clean
	mkdir -p build
	go build -o build/ideacrawler main.go

buildall: clean proto build

install: build
	cp build/ideacrawler $(GOBIN)/
