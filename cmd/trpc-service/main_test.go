package main

import (
	"bytes"
	"testing"
)

func TestParseServeArgs(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantAddr string
		wantHelp bool
		wantErr  bool
	}{
		{name: "default address", wantAddr: ":8080"},
		{name: "override address", args: []string{"-addr", "127.0.0.1:9090"}, wantAddr: "127.0.0.1:9090"},
		{name: "help", args: []string{"-h"}, wantHelp: true},
		{name: "unexpected argument", args: []string{"extra"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			addr, help, err := parseServeArgs(tt.args, &output)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseServeArgs() error = %v, wantErr %v", err, tt.wantErr)
			}
			if addr != tt.wantAddr || help != tt.wantHelp {
				t.Fatalf("parseServeArgs() = (%q, %v), want (%q, %v)", addr, help, tt.wantAddr, tt.wantHelp)
			}
		})
	}
}

func TestParseGatewayAndWorkerArgs(t *testing.T) {
	var output bytes.Buffer
	addr, consumer, help, err := parseGatewayArgs([]string{"-addr", "127.0.0.1:9090", "-consumer", "gateway-fixed"}, &output)
	if err != nil || help || addr != "127.0.0.1:9090" || consumer != "gateway-fixed" {
		t.Fatalf("parseGatewayArgs() = (%q, %q, %v, %v)", addr, consumer, help, err)
	}
	healthAddr, consumer, help, err := parseWorkerArgs([]string{"-health-addr", "127.0.0.1:9091", "-consumer", "worker-fixed"}, &output)
	if err != nil || help || healthAddr != "127.0.0.1:9091" || consumer != "worker-fixed" {
		t.Fatalf("parseWorkerArgs() = (%q, %q, %v, %v)", healthAddr, consumer, help, err)
	}
	if _, _, _, err := parseGatewayArgs([]string{"extra"}, &output); err == nil {
		t.Fatal("parseGatewayArgs() unexpectedly accepted a positional argument")
	}
	if _, _, _, err := parseWorkerArgs([]string{"extra"}, &output); err == nil {
		t.Fatal("parseWorkerArgs() unexpectedly accepted a positional argument")
	}
}
