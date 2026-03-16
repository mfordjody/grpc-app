package main

import (
	"context"
	"flag"
	"fmt"
	"grpc-app/logger"
	"grpc-app/mtls"
	pb "grpc-app/proto"
	"grpc-app/util"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	tlsv1 "github.com/dubbo-kubernetes/xds-api/extensions/transport_sockets/tls/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/wrapperspb"
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

	// Load mTLS certificates written by dubbo-agent to OUTPUT_CERTS.
	// File names are fixed: cert-chain.pem, key.pem, root-cert.pem.
	var serverOpts []grpc.ServerOption
	certDir := os.Getenv("OUTPUT_CERTS")
	if certDir == "" {
		certDir = "/var/lib/dubbo/data"
	}

	certFile := filepath.Join(certDir, "cert-chain.pem")
	keyFile := filepath.Join(certDir, "key.pem")

	if _, err := os.Stat(certFile); err == nil {
		if _, err := os.Stat(keyFile); err == nil {
			log.Printf("mTLS certificates found at %s, enabling mTLS for provider", certDir)
			tlsMgr := mtls.NewXDSCertificateManager(certDir)
			defer tlsMgr.Stop()

			downstreamCtx := &tlsv1.DownstreamTlsContext{
				CommonTlsContext: &tlsv1.CommonTlsContext{
					TlsCertificateCertificateProviderInstance: &tlsv1.CommonTlsContext_CertificateProviderInstance{
						InstanceName:    "default",
						CertificateName: "default",
					},
					ValidationContextType: &tlsv1.CommonTlsContext_CombinedValidationContext{
						CombinedValidationContext: &tlsv1.CommonTlsContext_CombinedCertificateValidationContext{
							ValidationContextCertificateProviderInstance: &tlsv1.CommonTlsContext_CertificateProviderInstance{
								InstanceName:    "default",
								CertificateName: "ROOTCA",
							},
						},
					},
				},
				RequireClientCertificate: &wrapperspb.BoolValue{Value: true},
			}

			certProvider, err := tlsMgr.ApplyDownstreamTLSContext(downstreamCtx)
			if err != nil {
				log.Printf("Warning: failed to apply mTLS context: %v, using insecure", err)
				serverOpts = append(serverOpts, grpc.Creds(insecure.NewCredentials()))
			} else {
				tlsCreds, err := certProvider.GetServerCredentials()
				if err != nil {
					log.Printf("Warning: failed to get TLS credentials: %v, using insecure", err)
					serverOpts = append(serverOpts, grpc.Creds(insecure.NewCredentials()))
				} else {
					serverOpts = append(serverOpts, grpc.Creds(tlsCreds))
					log.Printf("mTLS enabled for provider (certDir=%s)", certDir)
				}
			}
		} else {
			log.Printf("Key file not found at %s, using insecure", keyFile)
			serverOpts = append(serverOpts, grpc.Creds(insecure.NewCredentials()))
		}
	} else {
		log.Printf("Certificate file not found at %s, using insecure", certFile)
		serverOpts = append(serverOpts, grpc.Creds(insecure.NewCredentials()))
	}

	server := grpc.NewServer(serverOpts...)

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
