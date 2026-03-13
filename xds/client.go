package xds

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	discovery "github.com/dubbo-kubernetes/xds-api/service/discovery/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// BootstrapConfig holds parsed xDS bootstrap configuration.
type BootstrapConfig struct {
	ServerURI string
}

// ParseBootstrap reads and parses the xDS bootstrap JSON file.
func ParseBootstrap(path string) (*BootstrapConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read bootstrap file %s: %w", path, err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse bootstrap JSON: %w", err)
	}
	cfg := &BootstrapConfig{}
	if servers, ok := raw["xds_servers"].([]interface{}); ok && len(servers) > 0 {
		if s, ok := servers[0].(map[string]interface{}); ok {
			if uri, ok := s["server_uri"].(string); ok {
				cfg.ServerURI = uri
			}
		}
	}
	if cfg.ServerURI == "" {
		return nil, fmt.Errorf("no xds_servers[0].server_uri found in bootstrap")
	}
	return cfg, nil
}

// Client is a minimal xDS ADS client backed by xds-api types.
type Client struct {
	conn   *grpc.ClientConn
	stream discovery.AggregatedDiscoveryService_StreamAggregatedResourcesClient
	client discovery.AggregatedDiscoveryServiceClient
}

// NewClient dials the xDS management server and opens an ADS stream.
func NewClient(ctx context.Context, serverURI string) (*Client, error) {
	// Strip unix:// prefix for grpc dial
	addr := serverURI
	if strings.HasPrefix(addr, "unix://") {
		addr = "unix:" + strings.TrimPrefix(addr, "unix://")
	}

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial xDS server %s: %w", addr, err)
	}

	svcClient := discovery.NewAggregatedDiscoveryServiceClient(conn)
	stream, err := svcClient.StreamAggregatedResources(ctx)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open ADS stream: %w", err)
	}

	log.Printf("[xds] ADS stream established to %s", addr)
	return &Client{
		conn:   conn,
		stream: stream,
		client: svcClient,
	}, nil
}

// Subscribe sends an initial DiscoveryRequest for the given typeURL and resource names.
func (c *Client) Subscribe(typeURL string, resourceNames []string) error {
	return c.stream.Send(&discovery.DiscoveryRequest{
		TypeUrl:       typeURL,
		ResourceNames: resourceNames,
	})
}

// Recv receives the next DiscoveryResponse from the ADS stream.
func (c *Client) Recv() (*discovery.DiscoveryResponse, error) {
	return c.stream.Recv()
}

// Ack acknowledges a received DiscoveryResponse.
func (c *Client) Ack(resp *discovery.DiscoveryResponse) error {
	return c.stream.Send(&discovery.DiscoveryRequest{
		TypeUrl:       resp.TypeUrl,
		VersionInfo:   resp.VersionInfo,
		ResponseNonce: resp.Nonce,
	})
}

// Close shuts down the ADS stream and underlying connection.
func (c *Client) Close() {
	c.conn.Close()
}

