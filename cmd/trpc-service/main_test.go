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
