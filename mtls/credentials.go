package mtls

import (
	"github.com/dubbo-kubernetes/xds-api/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// NewClientCredentials returns client-side TLS credentials backed by
// file-based certificate watching via xds-api.
func NewClientCredentials(certFile, keyFile, caFile, serverName string) (*credentials.ClientOptions, error) {
	opts := &credentials.ClientOptions{
		CertFile:      certFile,
		KeyFile:       keyFile,
		CAFile:        caFile,
		ServerName:    serverName,
		FallbackCreds: insecure.NewCredentials(),
	}
	return opts, nil
}

// NewServerCredentials returns server-side TLS credentials backed by
// file-based certificate watching via xds-api.
func NewServerCredentials(certFile, keyFile, caFile string) (*credentials.ServerOptions, error) {
	opts := &credentials.ServerOptions{
		CertFile:      certFile,
		KeyFile:       keyFile,
		CAFile:        caFile,
		FallbackCreds: insecure.NewCredentials(),
	}
	return opts, nil
}
