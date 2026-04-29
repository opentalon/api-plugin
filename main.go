package main

import (
	"fmt"
	"log"
	"net"
	"os"

	apiplugin "github.com/opentalon/api-plugin/plugin"
	pluginpkg "github.com/opentalon/opentalon/pkg/plugin"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			log.Fatalf("api-plugin: panic: %v", r)
		}
	}()

	handler := apiplugin.NewHandler()

	// TCP mode: MCP_GRPC_PORT=50051 → listen on TCP; print handshake; serve.
	if port := os.Getenv("MCP_GRPC_PORT"); port != "" {
		ln, err := net.Listen("tcp", ":"+port)
		if err != nil {
			log.Fatalf("api-plugin: listen tcp :%s: %v", port, err)
		}
		hs := pluginpkg.Handshake{
			Version: pluginpkg.HandshakeVersion,
			Network: "tcp",
			Address: "0.0.0.0:" + port,
		}
		if _, err := fmt.Fprintln(os.Stdout, hs.String()); err != nil {
			log.Fatalf("api-plugin: write handshake: %v", err)
		}
		if err := pluginpkg.ServeListener(ln, handler); err != nil {
			log.Fatalf("api-plugin: serve: %v", err)
		}
		return
	}

	// Default: Unix socket mode.
	if err := pluginpkg.Serve(handler); err != nil {
		log.Fatalf("api-plugin: serve: %v", err)
	}
}
