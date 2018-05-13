# IdeaCrawler

IdeaCrawler is a blazing fast, highly effective, flexible client-server crawling framework written in Go. We built it to perform certain tasks better than existing crawlers do. Based on the tests we conducted, this crawler outperforms Scrapy and other alternatives, many times over.  

In addition to the features you would expect from a regular crawling library, it makes it very easy to do things like scaling across machines, crawling through VPNs, using cookies, using Chrome as a crawling backend, etc. It was written to act as a layer on top of a regular crawling library, with a lot of glue code pre-written. 

This framework also allows users to isolate crawling to a dedicated cluster, while still being controlled from a central location. Oh, and did we mention it's super fast?

<em>Footnote: IdeaCrawler is built by the engineering team at Ideas2IT. We've always believed in giving back to the open source community, and this crawler is a step in that direction.</em>

## Features
  * The server is written in Go.  We provide client libraries in Go and Python, but clients can be written in any language that Protobuf supports.
  * A single server can simultaneously run lots of crawl jobs submitted by multiple clients.  And a single client can use multiple servers running on different machines to distribute the crawling load,  so that none of the server IPs get blocked, etc.
  * The server can be configured to automatically deep crawl a site through a predefined URL regex crawl path, and send back to client only URLs matching a certain regex (for example, deep crawl product listing pages,  but send back only product pages)
  * Clients can choose whether server should crawl directly or use a headless chrome instance.
  * Customize UserAgent
  * Login before crawling, either using cookies, or using JS in when using headless chrome.
  * In Chrome mode,  there are prewritten JS functions for some common problems - like Scroll down,  or scroll down until infinite scroll stops loading new data, etc.  And it is trivial to add more custom functions into the server for reuse.
  * When not using Chrome,  it downloads just the actual pages,  but it can be configured to prefetch and cache related files,  inorder to mimic browser behaviour better.
  * Each crawl job can be configured to stop when a certain VPN network interface goes away,  or when we are logged out of a site.
  * ... there's more, we are preparing detailed documentation for all the features.

## Pre-requesites
  * Linux OS
  * Go compiler (>= v1.10) (from https://golang.org/dl/)
  * Dep dependency management tool (>= v0.4.1) (from https://github.com/golang/dep)
  * Protobuf compilers for Go and Python (only if you want to recompile the proto files)
  * (Headless) Chrome  (only for chrome mode)

## Setup instructions
### Client
  * For the client programs to compile,  you need to install grpc and protobuf packages.
  
  For Go, use
  
	go get -u github.com/golang/protobuf/protoc-gen-go
	go get google.golang.org/grpc
	
  For Python, use
  
	pip install grpcio-tools

### Server
  * Download the code using:
  
    `go get -u github.com/ideas2it/ideacrawler`
	  
  * Build using:
  
    `cd $GOPATH/src/github.com/ideas2it/ideacrawler && make install`
	  
  * This would install the server to `$GOPATH/bin/`.  Make sure this location is in your `PATH` environment variable.
  * Start the server with the below command:
  
    `ideacrawler [--ListenAddress <listen_ip_address>] [--ListenPort <port>] [--DialAddress <dial_ip_address>] [--LogPath <log_dirname>]`
	  
    where,
	  
  | Parameter         | Description                                                                                                          |
  | ----------------- | -------------------------------------------------------------------------------------------------------------------- |
  | `--ListenAddress` | IP address of the Interface to listen on, for client connections.  Defaults to `127.0.0.1`                           |
  | `--ListenPort`    | Port to listen on, for client connections. Defaults to `2345`                                                        |
  | `--DialAddress`   | IP address of the interface to dial on, for crawling. Useful when using a VPN.  Default is to use OS defined routes. |
  | `--LogPath`       | Creates separate log files for each submitted job, and some general logs.  Writes to stdout by default.              |

  * Once the server is up and running,  you can try running one of the example programs from the `examples` directory,  and start writing your own programs based on the examples.
