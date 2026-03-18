package main

import (
	"context"
	"encoding/json"
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/reflection"

	// Importing resolver registers the xds:/// scheme, the xds_weighted balancer,
	// and automatic per-connection mTLS via xDS CDS — zero TLS code in the app.
	_ "github.com/dubbo-kubernetes/xds-api/grpc/resolver"

	// DialOptions() injects the xDS transport credentials transparently.
	xdsserver "github.com/dubbo-kubernetes/xds-api/grpc/server"

	"grpc-app/logger"
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
		log.Println("Stopping test server...")
		testServer.GracefulStop()
	}
}

type cachedConnection struct {
	conn      *grpc.ClientConn
	createdAt time.Time
}

type testServerImpl struct {
	pb.UnimplementedEchoTestServiceServer
	connCache map[string]*cachedConnection
	connMutex sync.RWMutex
}

func startTestServer(port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Printf("Failed to listen on port %d: %v", port, err)
		return
	}

	testServer = grpc.NewServer()
	pb.RegisterEchoTestServiceServer(testServer, &testServerImpl{
		connCache: make(map[string]*cachedConnection),
	})
	reflection.Register(testServer)

	log.Printf("Test server listening on port %d for ForwardEcho (reflection enabled)", port)
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

	bootstrapPath := os.Getenv("GRPC_XDS_BOOTSTRAP")
	if bootstrapPath == "" {
		return nil, fmt.Errorf("GRPC_XDS_BOOTSTRAP environment variable is not set")
	}
	if _, err := os.Stat(bootstrapPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("bootstrap file does not exist: %s", bootstrapPath)
	}

	bootstrapData, err := os.ReadFile(bootstrapPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read bootstrap file: %v", err)
	}
	var bootstrapJSON map[string]interface{}
	if err := json.Unmarshal(bootstrapData, &bootstrapJSON); err != nil {
		return nil, fmt.Errorf("failed to parse bootstrap file: %v", err)
	}
	// Verify UDS socket exists if configured.
	if xdsServers, ok := bootstrapJSON["xds_servers"].([]interface{}); ok && len(xdsServers) > 0 {
		if srv, ok := xdsServers[0].(map[string]interface{}); ok {
			if serverURI, ok := srv["server_uri"].(string); ok {
				if strings.HasPrefix(serverURI, "unix://") {
					udsPath := strings.TrimPrefix(serverURI, "unix://")
					if _, err := os.Stat(udsPath); os.IsNotExist(err) {
						return nil, fmt.Errorf("UDS socket does not exist: %s", udsPath)
					}
				}
			}
		}
	}

	const maxConnectionAge = 10 * time.Second

	s.connMutex.RLock()
	cached, exists := s.connCache[req.Url]
	var conn *grpc.ClientConn
	if exists && cached != nil {
		conn = cached.conn
	}
	s.connMutex.RUnlock()

	if exists && conn != nil {
		state := conn.GetState()
		if state == connectivity.Shutdown {
			log.Printf("ForwardEcho: cached connection for %s is SHUTDOWN, removing", req.Url)
			s.connMutex.Lock()
			delete(s.connCache, req.Url)
			conn = nil
			exists = false
			s.connMutex.Unlock()
		} else if time.Since(cached.createdAt) > maxConnectionAge {
			log.Printf("ForwardEcho: cached connection for %s is too old, clearing", req.Url)
			s.connMutex.Lock()
			if c, ok := s.connCache[req.Url]; ok && c != nil && c.conn != nil {
				c.conn.Close()
			}
			delete(s.connCache, req.Url)
			conn = nil
			exists = false
			s.connMutex.Unlock()
		}
	}

	if !exists || conn == nil {
		s.connMutex.Lock()
		if c, ok := s.connCache[req.Url]; !ok || c == nil || c.conn == nil {
			log.Printf("ForwardEcho: creating new connection for %s...", req.Url)
			dialCtx, dialCancel := context.WithTimeout(context.Background(), 60*time.Second)
			// server.DialContext automatically injects xDS-aware transport
			// credentials — mTLS when control plane pushes DUBBO_MUTUAL,
			// plaintext otherwise.  No TLS code needed in the application.
			newConn, dialErr := xdsserver.DialContext(dialCtx, req.Url)
			dialCancel()
			if dialErr != nil {
				s.connMutex.Unlock()
				return nil, fmt.Errorf("failed to dial %s: %v", req.Url, dialErr)
			}
			s.connCache[req.Url] = &cachedConnection{conn: newConn, createdAt: time.Now()}
			conn = newConn
			log.Printf("ForwardEcho: cached connection for %s", req.Url)
		} else {
			conn = s.connCache[req.Url].conn
		}
		s.connMutex.Unlock()
	} else {
		log.Printf("ForwardEcho: reusing cached connection for %s (state: %v)", req.Url, conn.GetState())
	}

	// Wait for connection to be ready (up to 60 s).
	if conn.GetState() != connectivity.Ready {
		log.Printf("ForwardEcho: waiting for connection to be ready (60s)...")
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer waitCancel()
		for conn.GetState() != connectivity.Ready {
			if !conn.WaitForStateChange(waitCtx, conn.GetState()) {
				break
			}
		}
		log.Printf("ForwardEcho: connection state after wait: %v", conn.GetState())
	}

	client := pb.NewEchoServiceClient(conn)
	output := make([]string, 0, count)
	errorCount := 0
	firstError := ""

	log.Printf("ForwardEcho: sending %d requests...", count)
	for i := int32(0); i < count; i++ {
		reqCtx, reqCancel := context.WithTimeout(context.Background(), 30*time.Second)
		resp, err := client.Echo(reqCtx, &pb.EchoRequest{Message: fmt.Sprintf("Request %d", i+1)})
		reqCancel()

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

		log.Printf("ForwardEcho: request %d succeeded: Hostname=%s", i+1, resp.Hostname)
		lineParts := []string{fmt.Sprintf("[%d body] Hostname=%s", i, resp.Hostname)}
		if resp.ServiceVersion != "" {
			lineParts = append(lineParts, fmt.Sprintf("ServiceVersion=%s", resp.ServiceVersion))
		}
		if resp.Namespace != "" {
			lineParts = append(lineParts, fmt.Sprintf("Namespace=%s", resp.Namespace))
		}
		if resp.Ip != "" {
			lineParts = append(lineParts, fmt.Sprintf("IP=%s", resp.Ip))
		}
		if resp.Cluster != "" {
			lineParts = append(lineParts, fmt.Sprintf("Cluster=%s", resp.Cluster))
		}
		if resp.ServicePort > 0 {
			lineParts = append(lineParts, fmt.Sprintf("ServicePort=%d", resp.ServicePort))
		}
		output = append(output, strings.Join(lineParts, " "))
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