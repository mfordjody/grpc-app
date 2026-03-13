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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/reflection"
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

	// Set custom gRPC logger to filter out xDS informational logs
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
	// Connection cache: map from URL to cached connection
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

	// Check bootstrap configuration
	bootstrapPath := os.Getenv("GRPC_XDS_BOOTSTRAP")
	if bootstrapPath == "" {
		return nil, fmt.Errorf("GRPC_XDS_BOOTSTRAP environment variable is not set")
	}

	// Verify bootstrap file exists
	if _, err := os.Stat(bootstrapPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("bootstrap file does not exist: %s", bootstrapPath)
	}

	// Read bootstrap file to verify UDS socket
	bootstrapData, err := os.ReadFile(bootstrapPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read bootstrap file: %v", err)
	}

	var bootstrapJSON map[string]interface{}
	if err := json.Unmarshal(bootstrapData, &bootstrapJSON); err != nil {
		return nil, fmt.Errorf("failed to parse bootstrap file: %v", err)
	}

	// Extract UDS socket path
	var udsPath string
	if xdsServers, ok := bootstrapJSON["xds_servers"].([]interface{}); ok && len(xdsServers) > 0 {
		if server, ok := xdsServers[0].(map[string]interface{}); ok {
			if serverURI, ok := server["server_uri"].(string); ok {
				if strings.HasPrefix(serverURI, "unix://") {
					udsPath = strings.TrimPrefix(serverURI, "unix://")
					if _, err := os.Stat(udsPath); os.IsNotExist(err) {
						return nil, fmt.Errorf("UDS socket does not exist: %s", udsPath)
					}
				}
			}
		}
	}

	// Reuse connections to avoid creating new connections for each RPC call
	s.connMutex.RLock()
	cached, exists := s.connCache[req.Url]
	var conn *grpc.ClientConn
	if exists && cached != nil {
		conn = cached.conn
	}
	s.connMutex.RUnlock()

	// Clear connections older than 10 seconds to ensure latest config is used
	const maxConnectionAge = 10 * time.Second
	if exists && conn != nil {
		state := conn.GetState()
		if state == connectivity.Shutdown {
			log.Printf("ForwardEcho: cached connection for %s is SHUTDOWN, removing from cache", req.Url)
			s.connMutex.Lock()
			delete(s.connCache, req.Url)
			conn = nil
			exists = false
			s.connMutex.Unlock()
		} else if time.Since(cached.createdAt) > maxConnectionAge {
			log.Printf("ForwardEcho: cached connection for %s is too old (%v), clearing cache", req.Url, time.Since(cached.createdAt))
			s.connMutex.Lock()
			if cachedConn, stillExists := s.connCache[req.Url]; stillExists && cachedConn != nil && cachedConn.conn != nil {
				cachedConn.conn.Close()
			}
			delete(s.connCache, req.Url)
			conn = nil
			exists = false
			s.connMutex.Unlock()
		}
	}

	if !exists || conn == nil {
		s.connMutex.Lock()
		if cached, exists = s.connCache[req.Url]; !exists || cached == nil || cached.conn == nil {
			conn = nil
			log.Printf("ForwardEcho: creating new connection for %s...", req.Url)
			dialCtx, dialCancel := context.WithTimeout(context.Background(), 60*time.Second)
			var dialErr error
			conn, dialErr = grpc.DialContext(dialCtx, req.Url, grpc.WithTransportCredentials(insecure.NewCredentials()))
			dialCancel()
			if dialErr != nil {
				s.connMutex.Unlock()
				return nil, fmt.Errorf("failed to dial %s: %v", req.Url, dialErr)
			}
			s.connCache[req.Url] = &cachedConnection{
				conn:      conn,
				createdAt: time.Now(),
			}
			log.Printf("ForwardEcho: cached connection for %s", req.Url)
		}
		s.connMutex.Unlock()
	} else {
		log.Printf("ForwardEcho: reusing cached connection for %s (state: %v)", req.Url, conn.GetState())
	}

	initialState := conn.GetState()
	log.Printf("ForwardEcho: initial connection state: %v", initialState)

	if initialState != connectivity.Ready {
		log.Printf("ForwardEcho: waiting for connection to be ready (60 seconds)...")
		maxWait := 60 * time.Second
		currentState := initialState
		startTime := time.Now()

		for time.Since(startTime) < maxWait {
			if currentState == connectivity.Ready {
				break
			}
			if currentState == connectivity.Shutdown {
				break
			}
			remaining := maxWait - time.Since(startTime)
			if remaining <= 0 {
				break
			}
			waitTimeout := remaining
			if waitTimeout > 5*time.Second {
				waitTimeout = 5 * time.Second
			}
			stateCtx, stateCancel := context.WithTimeout(context.Background(), waitTimeout)
			if conn.WaitForStateChange(stateCtx, currentState) {
				currentState = conn.GetState()
				log.Printf("ForwardEcho: connection state changed to %v after %v", currentState, time.Since(startTime))
			}
			stateCancel()
		}

		finalState := conn.GetState()
		log.Printf("ForwardEcho: final connection state: %v (waited=%v)", finalState, time.Since(startTime))
		if finalState != connectivity.Ready {
			log.Printf("ForwardEcho: WARNING - connection is not READY (state=%v), proceeding anyway", finalState)
		}
	}

	client := pb.NewEchoServiceClient(conn)
	output := make([]string, 0, count)

	log.Printf("ForwardEcho: sending %d requests...", count)
	errorCount := 0
	firstError := ""
	for i := int32(0); i < count; i++ {
		echoReq := &pb.EchoRequest{
			Message: fmt.Sprintf("Request %d", i+1),
		}

		currentState := conn.GetState()
		log.Printf("ForwardEcho: sending request %d (connection state: %v)...", i+1, currentState)

		reqCtx, reqCancel := context.WithTimeout(context.Background(), 30*time.Second)
		reqStartTime := time.Now()
		resp, err := client.Echo(reqCtx, echoReq)
		duration := time.Since(reqStartTime)
		reqCancel()

		stateAfterRPC := conn.GetState()
		log.Printf("ForwardEcho: request %d completed in %v, connection state: %v (was %v)", i+1, duration, stateAfterRPC, currentState)

		if err != nil {
			log.Printf("ForwardEcho: request %d failed: %v", i+1, err)
			errorCount++
			if firstError == "" {
				firstError = err.Error()
			}
			errMsg := util.FormatGRPCError(err, i, count)
			output = append(output, errMsg)
			if i < count-1 {
				time.Sleep(2 * time.Second)
			}
			continue
		}

		if resp == nil {
			log.Printf("ForwardEcho: request %d failed: response is nil", i+1)
			output = append(output, fmt.Sprintf("[%d] Error: response is nil", i))
			continue
		}

		log.Printf("ForwardEcho: request %d succeeded: Hostname=%s ServiceVersion=%s Namespace=%s IP=%s",
			i+1, resp.Hostname, resp.ServiceVersion, resp.Namespace, resp.Ip)

		lineParts := []string{
			fmt.Sprintf("[%d body] Hostname=%s", i, resp.Hostname),
		}
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
		summary := fmt.Sprintf("ERROR:\nCode: Unknown\nMessage: %d/%d requests had errors; first error: %s", errorCount, count, firstError)
		output = append([]string{summary}, output...)
	}

	return &pb.ForwardEchoResponse{
		Output: output,
	}, nil
}
