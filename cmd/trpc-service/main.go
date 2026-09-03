package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/httpapi"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

const shutdownTimeout = 10 * time.Second

func main() {
	fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
	fmt.Println("multi-tenant node-based agent platform on tRPC-Agent-Go")
	if len(os.Args) <= 1 {
		return
	}
	if os.Args[1] == "-h" || os.Args[1] == "--help" {
		printUsage(os.Stderr)
		return
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "gateway":
		err = runGateway(os.Args[2:])
	case "worker":
		err = runWorker(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		printUsage(os.Stderr)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func printUsage(output io.Writer) {
	fmt.Fprintf(output, "usage:\n  %s serve [-addr 127.0.0.1:8080]\n  %s gateway [-addr 127.0.0.1:8080] [-consumer name]\n  %s worker [-health-addr :8081] [-consumer name]\n", os.Args[0], os.Args[0], os.Args[0])
}

func serve(args []string) error {
	addr, help, err := parseServeArgs(args, os.Stderr)
	if err != nil || help {
		return err
	}
	cfg, err := config.LoadForRole(config.RoleServe)
	if err != nil {
		return err
	}
	store, err := messaging.NewStore(*cfg.Messaging)
	if err != nil {
		return err
	}
	defer store.Close()
	router, err := newRouter(cfg)
	if err != nil {
		return err
	}
	adapters, err := newAdapters(cfg)
	if err != nil {
		return err
	}
	runtime, err := executor.New(cfg)
	if err != nil {
		return err
	}
	defer runtime.Close()
	workerConsumer, err := consumerName("worker")
	if err != nil {
		return err
	}
	gatewayConsumer, err := consumerName("gateway")
	if err != nil {
		return err
	}
	workerService, err := worker.New(store, runtime, workerConsumer)
	if err != nil {
		return err
	}
	gatewayService, err := gateway.NewWithAdapters(router, store, gatewayConsumer, adapters...)
	if err != nil {
		return err
	}
	return runCombined(addr, gatewayService, workerService, runtime)
}

func runGateway(args []string) error {
	addr, consumer, help, err := parseGatewayArgs(args, os.Stderr)
	if err != nil || help {
		return err
	}
	if consumer == "" {
		consumer, err = consumerName("gateway")
		if err != nil {
			return err
		}
	}
	cfg, err := config.LoadForRole(config.RoleGateway)
	if err != nil {
		return err
	}
	store, err := messaging.NewStore(*cfg.Messaging)
	if err != nil {
		return err
	}
	defer store.Close()
	router, err := newRouter(cfg)
	if err != nil {
		return err
	}
	adapters, err := newAdapters(cfg)
	if err != nil {
		return err
	}
	service, err := gateway.NewWithAdapters(router, store, consumer, adapters...)
	if err != nil {
		return err
	}
	return runGatewayServer(addr, service)
}

func runWorker(args []string) error {
	healthAddr, consumer, help, err := parseWorkerArgs(args, os.Stderr)
	if err != nil || help {
		return err
	}
	if consumer == "" {
		consumer, err = consumerName("worker")
		if err != nil {
			return err
		}
	}
	cfg, err := config.LoadForRole(config.RoleWorker)
	if err != nil {
		return err
	}
	store, err := messaging.NewStore(*cfg.Messaging)
	if err != nil {
		return err
	}
	defer store.Close()
	runtime, err := executor.New(cfg)
	if err != nil {
		return err
	}
	defer runtime.Close()
	service, err := worker.New(store, runtime, consumer)
	if err != nil {
		return err
	}
	return runWorkerServer(healthAddr, service)
}

func newRouter(cfg config.Config) (*routing.Router, error) {
	catalog, err := cfg.RoutingCatalog()
	if err != nil {
		return nil, err
	}
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		return nil, err
	}
	return routing.New(repository, cfg.IdentitySecret)
}

func newAdapters(cfg config.Config) ([]channels.Adapter, error) {
	catalog, resolver, err := cfg.GatewayCatalog()
	if err != nil {
		return nil, err
	}
	result := make([]channels.Adapter, 0, len(catalog.ChannelBindings))
	identities := make(map[[32]byte]string)
	for _, binding := range catalog.ChannelBindings {
		if !binding.Enabled || binding.Channel == "demo" {
			continue
		}
		var adapter channels.Adapter
		var identityValue string
		switch binding.Channel {
		case "telegram":
			token, resolveErr := resolver.Resolve(binding.CredentialRef)
			if resolveErr == nil {
				identityValue = token
				adapter, err = channels.NewTelegramAdapter(binding.ID, binding.ExternalAccountID, token, "")
			}
			if resolveErr != nil {
				adapter = &channels.UnavailableAdapter{BindingID: binding.ID}
			}
		case "wecom_aibot":
			botID, botErr := resolver.Resolve(binding.BotIDRef)
			secret, secretErr := resolver.Resolve(binding.BotSecretRef)
			if botErr == nil && secretErr == nil {
				identityValue = botID
				adapter, err = channels.NewWeComAdapter(binding.ID, binding.ExternalAccountID, botID, secret, "")
			}
			if botErr != nil || secretErr != nil {
				adapter = &channels.UnavailableAdapter{BindingID: binding.ID}
			}
		}
		if err != nil {
			return nil, err
		}
		if identityValue != "" {
			digest := sha256.Sum256([]byte(binding.Channel + "\x00" + identityValue))
			if prior, exists := identities[digest]; exists {
				return nil, fmt.Errorf("enabled bindings %q and %q reuse the same IM bot", prior, binding.ID)
			}
			identities[digest] = binding.ID
		}
		result = append(result, adapter)
	}
	return result, nil
}

func runCombined(addr string, gatewayService *gateway.Service, workerService *worker.Worker, runtime *executor.Runtime) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	gatewayErr := make(chan error, 1)
	workerErr := make(chan error, 1)
	var loops sync.WaitGroup
	loops.Add(2)
	go func() { defer loops.Done(); gatewayErr <- gatewayService.Run(ctx) }()
	go func() { defer loops.Done(); workerErr <- workerService.Run(ctx) }()
	server := newHTTPServer(addr, httpapi.NewHandler(combinedBackend{gateway: gatewayService, worker: workerService}))
	serverErr := listen(server)

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-gatewayErr:
	case runErr = <-workerErr:
	case runErr = <-serverErr:
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	serverShutdownErr := server.Shutdown(shutdownCtx)
	_ = gatewayService.Close()
	_ = workerService.Close()
	loops.Wait()
	runtimeErr := runtime.Close()
	return errors.Join(runErr, serverShutdownErr, runtimeErr)
}

type combinedBackend struct {
	gateway *gateway.Service
	worker  *worker.Worker
}

func (b combinedBackend) Ready(ctx context.Context) error {
	if err := b.gateway.Ready(ctx); err != nil {
		return err
	}
	return b.worker.Ready(ctx)
}

func (b combinedBackend) Handle(ctx context.Context, inbound message.InboundMessage) (message.OutboundMessage, error) {
	return b.gateway.Handle(ctx, inbound)
}

func (b combinedBackend) Accept(ctx context.Context, inbound message.InboundMessage) (channels.AcceptResult, error) {
	return b.gateway.Accept(ctx, inbound)
}

func (b combinedBackend) Snapshot(ctx context.Context, channel, bindingID, messageID string) (messaging.Snapshot, error) {
	return b.gateway.Snapshot(ctx, channel, bindingID, messageID)
}

func runGatewayServer(addr string, service *gateway.Service) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runErr := make(chan error, 1)
	var loop sync.WaitGroup
	loop.Add(1)
	go func() { defer loop.Done(); runErr <- service.Run(ctx) }()
	server := newHTTPServer(addr, httpapi.NewHandler(service))
	serverErr := listen(server)
	var err error
	select {
	case <-ctx.Done():
	case err = <-runErr:
	case err = <-serverErr:
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := errors.Join(server.Shutdown(shutdownCtx), service.Close())
	loop.Wait()
	return errors.Join(err, shutdownErr)
}

func runWorkerServer(addr string, service *worker.Worker) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runErr := make(chan error, 1)
	var loop sync.WaitGroup
	loop.Add(1)
	go func() { defer loop.Done(); runErr <- service.Run(ctx) }()
	server := newHTTPServer(addr, httpapi.NewHealthHandler(service))
	serverErr := listen(server)
	var err error
	select {
	case <-ctx.Done():
	case err = <-runErr:
	case err = <-serverErr:
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := errors.Join(server.Shutdown(shutdownCtx), service.Close())
	loop.Wait()
	return errors.Join(err, shutdownErr)
}

func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
}

func listen(server *http.Server) <-chan error {
	result := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		result <- err
	}()
	return result
}

func consumerName(role string) (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}
	suffix, err := identity.RequestID()
	if err != nil {
		return "", err
	}
	hostname = strings.NewReplacer(" ", "_", ":", "_").Replace(hostname)
	return fmt.Sprintf("%s-%s-%d-%s", role, hostname, os.Getpid(), suffix), nil
}

func parseServeArgs(args []string, output io.Writer) (string, bool, error) {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(output)
	addr := flags.String("addr", "127.0.0.1:8080", "HTTP listen address")
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

func parseGatewayArgs(args []string, output io.Writer) (string, string, bool, error) {
	flags := flag.NewFlagSet("gateway", flag.ContinueOnError)
	flags.SetOutput(output)
	addr := flags.String("addr", "127.0.0.1:8080", "HTTP listen address")
	consumer := flags.String("consumer", "", "Redis reply consumer name")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return "", "", true, nil
		}
		return "", "", false, err
	}
	if flags.NArg() != 0 {
		return "", "", false, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return *addr, *consumer, false, nil
}

func parseWorkerArgs(args []string, output io.Writer) (string, string, bool, error) {
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	flags.SetOutput(output)
	addr := flags.String("health-addr", ":8081", "Worker health listen address")
	consumer := flags.String("consumer", "", "Redis task consumer name")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return "", "", true, nil
		}
		return "", "", false, err
	}
	if flags.NArg() != 0 {
		return "", "", false, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return *addr, *consumer, false, nil
}
