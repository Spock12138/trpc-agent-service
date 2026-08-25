package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/httpapi"
)

func main() {
	fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
	fmt.Println("multi-tenant node-based agent platform on tRPC-Agent-Go")
	if len(os.Args) <= 1 {
		return
	}
	if os.Args[1] == "-h" || os.Args[1] == "--help" {
		fmt.Fprintf(os.Stderr, "usage: %s serve [-addr :8080]\n", os.Args[0])
		return
	}
	if os.Args[1] != "serve" {
		fmt.Fprintf(os.Stderr, "unknown command %q\nusage: %s serve [-addr :8080]\n", os.Args[1], os.Args[0])
		os.Exit(2)
	}
	if err := serve(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serve(args []string) error {
	addr, help, err := parseServeArgs(args, os.Stderr)
	if err != nil || help {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	runtime, err := executor.New(cfg)
	if err != nil {
		return err
	}
	defer runtime.Close()

	server := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewHandler(runtime),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)
	select {
	case err := <-serverErr:
		return err
	case <-stop:
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			return err
		}
		return runtime.Close()
	}
}

func parseServeArgs(args []string, output io.Writer) (string, bool, error) {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(output)
	addr := flags.String("addr", ":8080", "HTTP listen address")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return "", true, nil
		}
		return "", false, err
	}
	if flags.NArg() != 0 {
		return "", false, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return *addr, false, nil
}
