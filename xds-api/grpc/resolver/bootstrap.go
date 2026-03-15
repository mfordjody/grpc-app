package resolver

import (
	"encoding/json"
	"fmt"
	"os"

	corev1 "github.com/dubbo-kubernetes/xds-api/core/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// BootstrapConfig holds parsed xDS bootstrap configuration.
type BootstrapConfig struct {
	ServerURI string
	Node      *corev1.Node
}

// ParseBootstrap reads and parses the xDS bootstrap JSON file.
func ParseBootstrap(path string) (*BootstrapConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read bootstrap file %s: %w", path, err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse bootstrap JSON: %w", err)
	}

	cfg := &BootstrapConfig{}

	// Parse xds_servers[0].server_uri
	if serversRaw, ok := raw["xds_servers"]; ok {
		var servers []map[string]json.RawMessage
		if err := json.Unmarshal(serversRaw, &servers); err == nil && len(servers) > 0 {
			if uriRaw, ok := servers[0]["server_uri"]; ok {
				var uri string
				if err := json.Unmarshal(uriRaw, &uri); err == nil {
					cfg.ServerURI = uri
				}
			}
		}
	}
	if cfg.ServerURI == "" {
		return nil, fmt.Errorf("no xds_servers[0].server_uri found in bootstrap")
	}

	// Parse node using protojson (Node contains protobuf Struct for metadata)
	if nodeRaw, ok := raw["node"]; ok {
		node := &corev1.Node{}
		if err := protojson.Unmarshal(nodeRaw, node); err == nil {
			cfg.Node = node
		}
	}

	return cfg, nil
}
