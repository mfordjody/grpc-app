package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/reflection"

	// NewGRPCServer handles all xDS TLS lifecycle automatically.
	"github.com/dubbo-kubernetes/xds-api/grpc/server"

	"grpc-app/logger"
	pb "grpc-app/proto"
	"grpc-app/util"
)

var (
	port = flag.Int("port", 17070, "gRPC server port")
)

func waitForBootstrapFile(bootstrapPath string, maxWait time.Duration) error {
	log.Printf("Waiting for bootstrap file to exist: %s (max wait: %v)", bootstrapPath, maxWait)
	ctx, cancel := context.WithTimeout(context.Background(), maxWait)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	startTime := time.Now()
	for {
		if info, err := os.Stat(bootstrapPath); err == nil && info.Size() > 0 {
			log.Printf("Bootstrap file found after %v: %s", time.Since(startTime), bootstrapPath)
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for bootstrap file: %s (waited %v)", bootstrapPath, time.Since(startTime))
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
	serviceVersion := util.FirstNonEmpty(os.Getenv("SERVICE_VERSION"), os.Getenv("POD_VERSION"), os.Getenv("VERSION"))
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

	addr := fmt.Sprintf("0.0.0.0:%d", *port)

	// --- Pure business logic below. No TLS, no credentials, no watcher. ---
	// NewGRPCServer transparently handles plaintext ↔ mTLS switching based on
	// PeerAuthentication policies pushed by the xDS control plane.
	srv := server.NewGRPCServer(addr, bootstrapPath)

	es := &echoServer{
		hostname:       hostname,
		serviceVersion: serviceVersion,
		namespace:      namespace,
		instanceIP:     instanceIP,
		cluster:        cluster,
		servicePort:    servicePort,
	}
	srv.RegisterService(&pb.EchoService_ServiceDesc, es)
	srv.RegisterService(&pb.EchoTestService_ServiceDesc, es)

	// Register gRPC reflection using the managed server's internal registration
	// hook so grpcurl can introspect without a proto file.
	srv.RegisterHook(func(s *grpc.Server) {
		reflection.Register(s)
	})

	log.Printf("Starting gRPC provider on %s (hostname: %s)", addr, hostname)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := srv.ServeContext(ctx); err != nil && err != context.Canceled {
		log.Fatalf("provider exited: %v", err)
	}
	log.Println("provider shut down cleanly")
}
