package main

import (
	"context"
	"flag"
	"fmt"
	"grpc-app/logger"
	pb "grpc-app/proto"
	"grpc-app/util"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/reflection"
)

var (
	port = flag.Int("port", 17070, "gRPC server port")
)

// waitForBootstrapFile waits for the grpc-bootstrap.json file to exist
// This is necessary because the dubbo-proxy sidecar needs time to generate the file
func waitForBootstrapFile(bootstrapPath string, maxWait time.Duration) error {
	log.Printf("Waiting for bootstrap file to exist: %s (max wait: %v)", bootstrapPath, maxWait)

	ctx, cancel := context.WithTimeout(context.Background(), maxWait)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	startTime := time.Now()
	for {
		// Check if file exists and is not empty
		if info, err := os.Stat(bootstrapPath); err == nil && info.Size() > 0 {
			log.Printf("Bootstrap file found after %v: %s", time.Since(startTime), bootstrapPath)
			return nil
		}

		// Check for timeout
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for bootstrap file: %s (waited %v)", bootstrapPath, time.Since(startTime))
		case <-ticker.C:
			// Continue waiting
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

	return &pb.ForwardEchoResponse{
		Output: output,
	}, nil
}

func main() {
	flag.Parse()

	// Set custom gRPC logger to filter out xDS informational logs
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

	// Get bootstrap file path from environment variable or use default
	bootstrapPath := os.Getenv("GRPC_XDS_BOOTSTRAP")
	if bootstrapPath == "" {
		bootstrapPath = "/etc/dubbo/proxy/grpc-bootstrap.json"
		log.Printf("GRPC_XDS_BOOTSTRAP not set, using default: %s", bootstrapPath)
	}

	// Wait for bootstrap file to exist before starting server
	// The dubbo-proxy sidecar needs time to generate this file
	if err := waitForBootstrapFile(bootstrapPath, 60*time.Second); err != nil {
		log.Fatalf("Failed to wait for bootstrap file: %v", err)
	}

	server := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))

	es := &echoServer{
		hostname:       hostname,
		serviceVersion: serviceVersion,
		namespace:      namespace,
		instanceIP:     instanceIP,
		cluster:        cluster,
		servicePort:    servicePort,
	}
	pb.RegisterEchoServiceServer(server, es)
	pb.RegisterEchoTestServiceServer(server, es)
	// Enable reflection API for grpcurl to discover services
	reflection.Register(server)

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("Starting gRPC server on port %d (hostname: %s)", *port, hostname)

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutting down server...")
		server.GracefulStop()
	}()

	if err := server.Serve(lis); err != nil {
		if !strings.Contains(err.Error(), "use of closed network connection") {
			log.Fatalf("Failed to serve: %v", err)
		}
		log.Printf("Server stopped: %v", err)
	}
}
