package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	xdsserver "github.com/dubbo-kubernetes/xds-api/grpc/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/reflection"
	"grpc-app/logger"
	"grpc-app/mtls"
	pb "grpc-app/proto"
	"grpc-app/util"
)

var (
	port = flag.Int("port", 17070, "gRPC server port")
)

// waitForBootstrapFile waits for the grpc-bootstrap.json file to exist.
func waitForBootstrapFile(bootstrapPath string, maxWait time.Duration) error {
	log.Printf("Waiting for bootstrap file: %s (max %v)", bootstrapPath, maxWait)
	ctx, cancel := context.WithTimeout(context.Background(), maxWait)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	start := time.Now()
	for {
		if info, err := os.Stat(bootstrapPath); err == nil && info.Size() > 0 {
			log.Printf("Bootstrap file ready after %v", time.Since(start))
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for bootstrap file %s (waited %v)", bootstrapPath, time.Since(start))
		case <-ticker.C:
		}
	}
}

type echoServer struct {
	pb.UnimplementedEchoServiceServer
	pb.UnimplementedEchoTestServiceServer
	hostname       string
	serviceVersion string
	namespace      string
	instanceIP     string
	cluster        string
	servicePort    int
}

func (s *echoServer) Echo(ctx context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request is nil")
	}
	log.Printf("Received: %v", req.Message)
	return &pb.EchoResponse{
		Message:        req.Message,
		Hostname:       s.hostname,
		ServiceVersion: s.serviceVersion,
		Namespace:      s.namespace,
		Ip:             s.instanceIP,
		Cluster:        s.cluster,
		ServicePort:    int32(s.servicePort),
	}, nil
}

func (s *echoServer) StreamEcho(req *pb.EchoRequest, stream pb.EchoService_StreamEchoServer) error {
	if req == nil {
		return fmt.Errorf("request is nil")
	}
	if stream == nil {
		return fmt.Errorf("stream is nil")
	}
	log.Printf("StreamEcho received: %v", req.Message)
	for i := 0; i < 3; i++ {
		if err := stream.Send(&pb.EchoResponse{
			Message:  fmt.Sprintf("%s [%d]", req.Message, i),
			Hostname: s.hostname,
		}); err != nil {
			log.Printf("StreamEcho send error: %v", err)
			return err
		}
	}
	return nil
}

func (s *echoServer) ForwardEcho(ctx context.Context, req *pb.ForwardEchoRequest) (*pb.ForwardEchoResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request is nil")
	}
	count := req.Count
	if count < 0 {
		count = 0
	}
	if count > 100 {
		count = 100
	}
	log.Printf("ForwardEcho called: url=%s, count=%d", req.Url, count)
	output := make([]string, 0, count)
	for i := int32(0); i < count; i++ {
		line := fmt.Sprintf("[%d body] Hostname=%s ServiceVersion=%s ServicePort=%d Namespace=%s",
			i, s.hostname, s.serviceVersion, s.servicePort, s.namespace)
		if s.instanceIP != "" {
			line += fmt.Sprintf(" IP=%s", s.instanceIP)
		}
		if s.cluster != "" {
			line += fmt.Sprintf(" Cluster=%s", s.cluster)
		}
		output = append(output, line)
	}
	return &pb.ForwardEchoResponse{Output: output}, nil
}

// buildServerOpts resolves gRPC server credentials from the xDS inbound TLS config.
func buildServerOpts(tlsCfg *xdsserver.InboundTLSConfig, certDir string) []grpc.ServerOption {
	if tlsCfg == nil || tlsCfg.Mode != xdsserver.TLSModeMTLS {
		log.Printf("[provider] xDS inbound listener: plaintext mode")
		return []grpc.ServerOption{grpc.Creds(insecure.NewCredentials())}
	}

	log.Printf("[provider] xDS inbound listener: mTLS mode, loading certs from %s", certDir)
	tlsMgr := mtls.NewXDSCertificateManager(certDir)
	certProvider, err := tlsMgr.ApplyDownstreamTLSContext(tlsCfg.Downstream)
	if err != nil {
		log.Printf("[provider] failed to apply DownstreamTlsContext: %v, falling back to insecure", err)
		tlsMgr.Stop()
		return []grpc.ServerOption{grpc.Creds(insecure.NewCredentials())}
	}
	creds, err := certProvider.GetServerCredentials()
	if err != nil {
		log.Printf("[provider] failed to build server TLS credentials: %v, falling back to insecure", err)
		tlsMgr.Stop()
		return []grpc.ServerOption{grpc.Creds(insecure.NewCredentials())}
	}
	log.Printf("[provider] mTLS server credentials ready")
	return []grpc.ServerOption{grpc.Creds(creds)}
}

func runServer(opts []grpc.ServerOption, es *echoServer, listenAddr string, stopCh <-chan struct{}) {
	server := grpc.NewServer(opts...)
	pb.RegisterEchoServiceServer(server, es)
	pb.RegisterEchoTestServiceServer(server, es)
	reflection.Register(server)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", listenAddr, err)
	}
	log.Printf("[provider] gRPC server listening on %s", listenAddr)

	go func() {
		<-stopCh
		log.Printf("[provider] stopping gRPC server")
		server.GracefulStop()
	}()

	if err := server.Serve(lis); err != nil {
		if !strings.Contains(err.Error(), "use of closed network connection") {
			log.Printf("[provider] server error: %v", err)
		}
	}
}

func main() {
	flag.Parse()

	grpclog.SetLoggerV2(&logger.GrpcLogger{
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	})

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	namespace := util.FirstNonEmpty(os.Getenv("SERVICE_NAMESPACE"), os.Getenv("POD_NAMESPACE"), "default")
	serviceVersion := util.FirstNonEmpty(
		os.Getenv("SERVICE_VERSION"),
		os.Getenv("POD_VERSION"),
		os.Getenv("VERSION"),
	)
	if serviceVersion == "" {
		serviceVersion = "unknown"
	}
	cluster := os.Getenv("SERVICE_CLUSTER")
	instanceIP := os.Getenv("INSTANCE_IP")
	servicePort := *port
	if sp := os.Getenv("SERVICE_PORT"); sp != "" {
		if parsed, err := strconv.Atoi(sp); err == nil {
			servicePort = parsed
		}
	}

	bootstrapPath := os.Getenv("GRPC_XDS_BOOTSTRAP")
	if bootstrapPath == "" {
		bootstrapPath = "/etc/dubbo/proxy/grpc-bootstrap.json"
		log.Printf("GRPC_XDS_BOOTSTRAP not set, using default: %s", bootstrapPath)
	}

	if err := waitForBootstrapFile(bootstrapPath, 60*time.Second); err != nil {
		log.Fatalf("Failed to wait for bootstrap file: %v", err)
	}

	certDir := os.Getenv("OUTPUT_CERTS")
	if certDir == "" {
		certDir = "/var/lib/dubbo/data"
	}

	// Inbound listen address for the xDS listener name template.
	listenAddr := fmt.Sprintf("0.0.0.0:%d", *port)

	// Subscribe to the xDS inbound listener to get the initial TLS decision
	// before starting the gRPC server. This prevents the TLS mismatch that
	// occurs when provider statically enables mTLS but consumer uses insecure.
	watcher := xdsserver.NewWatcher(listenAddr, bootstrapPath)
	watcher.Start()
	defer watcher.Close()

	initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
	initTLS, err := watcher.WaitForInitial(initCtx)
	initCancel()
	if err != nil {
		// No xDS response in time — start with insecure (safe default).
		log.Printf("[provider] xDS inbound listener timeout (%v), starting with insecure", err)
		initTLS = &xdsserver.InboundTLSConfig{Mode: xdsserver.TLSModePlaintext}
	}

	es := &echoServer{
		hostname:       hostname,
		serviceVersion: serviceVersion,
		namespace:      namespace,
		instanceIP:     instanceIP,
		cluster:        cluster,
		servicePort:    servicePort,
	}

	// OS signal channel for clean shutdown.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	currentMode := initTLS.Mode
	serverOpts := buildServerOpts(initTLS, certDir)

	for {
		stopCh := make(chan struct{})
		go runServer(serverOpts, es, listenAddr, stopCh)

		// Wait for TLS config change or OS signal.
		select {
		case <-sigChan:
			log.Println("[provider] shutting down")
			close(stopCh)
			return

		case update := <-watcher.UpdateCh():
			if update.Mode == currentMode {
				// Same mode, no restart needed.
				continue
			}
			log.Printf("[provider] xDS TLS mode changed (%v -> %v), restarting server",
				currentMode, update.Mode)
			close(stopCh)
			// Brief pause to allow port to be released.
			time.Sleep(200 * time.Millisecond)
			currentMode = update.Mode
			serverOpts = buildServerOpts(update, certDir)
		}
	}
}
