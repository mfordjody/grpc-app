package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	xdsresolver "github.com/dubbo-kubernetes/xds-api/grpc/resolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/reflection"
	"grpc-app/logger"
	"grpc-app/mtls"
	pb "grpc-app/proto"
	"grpc-app/util"
)

var (
	port       = flag.Int("port", 17171, "gRPC server port for ForwardEcho testing")
	testServer *grpc.Server
)

func main() {
	flag.Parse()

	grpclog.SetLoggerV2(&logger.GrpcLogger{
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	})

	go startTestServer(*port)

	log.Printf("Consumer running. Test server listening on port %d for ForwardEcho", *port)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("Shutting down...")

	if testServer != nil {
		testServer.GracefulStop()
	}
}

type cachedConnection struct {
	conn      *grpc.ClientConn
	createdAt time.Time
	hasTLS    bool
}

type testServerImpl struct {
	pb.UnimplementedEchoTestServiceServer
	connCache map[string]*cachedConnection
	connMutex sync.RWMutex
}

func startTestServer(p int) {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", p))
	if err != nil {
		log.Printf("Failed to listen on port %d: %v", p, err)
		return
	}
	testServer = grpc.NewServer()
	pb.RegisterEchoTestServiceServer(testServer, &testServerImpl{
		connCache: make(map[string]*cachedConnection),
	})
	reflection.Register(testServer)
	log.Printf("Test server listening on port %d", p)
	if err := testServer.Serve(lis); err != nil {
		log.Printf("Test server error: %v", err)
	}
}

func (s *testServerImpl) ForwardEcho(ctx context.Context, req *pb.ForwardEchoRequest) (*pb.ForwardEchoResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request is nil")
	}
	if req.Url == "" {
		return nil, fmt.Errorf("url is required")
	}

	count := req.Count
	if count < 0 {
		count = 0
	}
	if count > 100 {
		count = 100
	}
	log.Printf("ForwardEcho: url=%s, count=%d", req.Url, count)

	// certDir is where dubbo-agent writes cert-chain.pem / key.pem / root-cert.pem.
	certDir := os.Getenv("OUTPUT_CERTS")
	if certDir == "" {
		certDir = "/var/lib/dubbo/data"
	}

	// Determine whether xDS has pushed a TLS config for this target.
	currentlyHasTLS := xdsresolver.GetClusterTLSForTarget(req.Url) != nil

	const maxAge = 10 * time.Second

	s.connMutex.RLock()
	cached := s.connCache[req.Url]
	s.connMutex.RUnlock()

	var conn *grpc.ClientConn
	if cached != nil {
		state := cached.conn.GetState()
		stale := time.Since(cached.createdAt) > maxAge
		tlsChanged := cached.hasTLS != currentlyHasTLS
		if state == connectivity.Shutdown || stale || tlsChanged {
			s.connMutex.Lock()
			if c, ok := s.connCache[req.Url]; ok {
				c.conn.Close()
				delete(s.connCache, req.Url)
			}
			s.connMutex.Unlock()
			cached = nil
		} else {
			conn = cached.conn
		}
	}

	if conn == nil {
		s.connMutex.Lock()
		if c, ok := s.connCache[req.Url]; ok && c != nil && c.conn.GetState() != connectivity.Shutdown {
			conn = c.conn
			s.connMutex.Unlock()
		} else {
			log.Printf("ForwardEcho: creating new connection to %s (hasTLS=%v)", req.Url, currentlyHasTLS)

			// Build transport credentials.
			// xDS CDS pushes UpstreamTlsContext when DestinationRule sets DUBBO_MUTUAL.
			// We use the mtls package (backed by dubbo-agent certificate files) to build
			// standard Go TLS credentials — no envoy/istio dependency.
			transportCreds := grpc.WithTransportCredentials(insecure.NewCredentials())
			if tlsCtx := xdsresolver.GetClusterTLSForTarget(req.Url); tlsCtx != nil {
				log.Printf("ForwardEcho: xDS supplied UpstreamTlsContext for %s, enabling mTLS (SNI=%s)", req.Url, tlsCtx.Sni)
				tlsMgr := mtls.NewXDSCertificateManager(certDir)
				defer tlsMgr.Stop()
				certProvider, err := tlsMgr.ApplyUpstreamTLSContext(tlsCtx)
				if err != nil {
					log.Printf("ForwardEcho: failed to apply xDS TLS context: %v, falling back to insecure", err)
				} else {
					creds, err := certProvider.GetClientCredentials()
					if err != nil {
						log.Printf("ForwardEcho: failed to build TLS credentials: %v, falling back to insecure", err)
					} else {
						transportCreds = grpc.WithTransportCredentials(creds)
						log.Printf("ForwardEcho: mTLS credentials applied for %s", req.Url)
					}
				}
			} else {
				log.Printf("ForwardEcho: no xDS TLS context for %s, using insecure", req.Url)
			}

			dialCtx, dialCancel := context.WithTimeout(context.Background(), 60*time.Second)
			newConn, dialErr := grpc.DialContext(
				dialCtx,
				req.Url,
				transportCreds,
				grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
				grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
					// Strip the #N suffix added by the xDS weighted_target balancer.
					if idx := strings.LastIndex(addr, "#"); idx >= 0 {
						addr = addr[:idx]
					}
					return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
				}),
			)
			dialCancel()
			if dialErr != nil {
				s.connMutex.Unlock()
				return nil, fmt.Errorf("failed to dial %s: %v", req.Url, dialErr)
			}
			conn = newConn
			s.connCache[req.Url] = &cachedConnection{
				conn:      conn,
				createdAt: time.Now(),
				hasTLS:    currentlyHasTLS,
			}
			log.Printf("ForwardEcho: connection cached for %s", req.Url)
			s.connMutex.Unlock()
		}
	} else {
		log.Printf("ForwardEcho: reusing cached connection for %s (state: %v)", req.Url, conn.GetState())
	}

	// Wait for READY.
	if conn.GetState() != connectivity.Ready {
		log.Printf("ForwardEcho: waiting for connection READY...")
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer waitCancel()
		for conn.GetState() != connectivity.Ready {
			if !conn.WaitForStateChange(waitCtx, conn.GetState()) {
				break
			}
		}
		log.Printf("ForwardEcho: final connection state: %v", conn.GetState())
	}

	client := pb.NewEchoServiceClient(conn)
	output := make([]string, 0, count)
	errorCount := 0
	firstError := ""

	log.Printf("ForwardEcho: sending %d requests...", count)
	for i := int32(0); i < count; i++ {
		reqCtx, reqCancel := context.WithTimeout(context.Background(), 30*time.Second)
		start := time.Now()
		resp, err := client.Echo(reqCtx, &pb.EchoRequest{Message: fmt.Sprintf("Request %d", i+1)})
		dur := time.Since(start)
		reqCancel()

		log.Printf("ForwardEcho: request %d completed in %v (state: %v)", i+1, dur, conn.GetState())

		if err != nil {
			log.Printf("ForwardEcho: request %d failed: %v", i+1, err)
			errorCount++
			if firstError == "" {
				firstError = err.Error()
			}
			output = append(output, util.FormatGRPCError(err, i, count))
			if i < count-1 {
				time.Sleep(2 * time.Second)
			}
			continue
		}
		if resp == nil {
			output = append(output, fmt.Sprintf("[%d] Error: response is nil", i))
			continue
		}

		log.Printf("ForwardEcho: request %d succeeded: Hostname=%s ServiceVersion=%s Namespace=%s IP=%s",
			i+1, resp.Hostname, resp.ServiceVersion, resp.Namespace, resp.Ip)

		parts := []string{fmt.Sprintf("[%d body] Hostname=%s", i, resp.Hostname)}
		if resp.ServiceVersion != "" {
			parts = append(parts, fmt.Sprintf("ServiceVersion=%s", resp.ServiceVersion))
		}
		if resp.Namespace != "" {
			parts = append(parts, fmt.Sprintf("Namespace=%s", resp.Namespace))
		}
		if resp.Ip != "" {
			parts = append(parts, fmt.Sprintf("IP=%s", resp.Ip))
		}
		if resp.Cluster != "" {
			parts = append(parts, fmt.Sprintf("Cluster=%s", resp.Cluster))
		}
		if resp.ServicePort > 0 {
			parts = append(parts, fmt.Sprintf("ServicePort=%d", resp.ServicePort))
		}
		output = append(output, strings.Join(parts, " "))

		if i < count-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	log.Printf("ForwardEcho: completed %d requests", count)

	if errorCount > 0 && errorCount == int(count) && firstError != "" {
		summary := fmt.Sprintf("ERROR:\nCode: Unknown\nMessage: %d/%d requests had errors; first error: %s",
			errorCount, count, firstError)
		output = append([]string{summary}, output...)
	}

	return &pb.ForwardEchoResponse{Output: output}, nil
}
