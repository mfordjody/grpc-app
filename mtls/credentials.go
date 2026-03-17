package mtls

import (
	stdtls "crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc/credentials"
)

// CertificateProvider manages TLS certificates from xDS
type CertificateProvider struct {
	mu              sync.RWMutex
	certFile        string
	keyFile         string
	caFile          string
	lastModTime     map[string]time.Time
	tlsCert         *stdtls.Certificate
	caCertPool      *x509.CertPool
	refreshInterval time.Duration
	stopCh          chan struct{}
	stopped         bool
}

// NewCertificateProvider creates a new certificate provider
func NewCertificateProvider(certFile, keyFile, caFile string) *CertificateProvider {
	cp := &CertificateProvider{
		certFile:        certFile,
		keyFile:         keyFile,
		caFile:          caFile,
		lastModTime:     make(map[string]time.Time),
		refreshInterval: 5 * time.Second,
		stopCh:          make(chan struct{}),
	}
	return cp
}

// Start begins monitoring certificate files for changes
func (cp *CertificateProvider) Start() error {
	// Initial load
	if err := cp.loadCertificates(); err != nil {
		return err
	}

	// Start watcher goroutine
	go cp.watchCertificates()
	return nil
}

// Stop stops the certificate watcher
func (cp *CertificateProvider) Stop() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if !cp.stopped {
		close(cp.stopCh)
		cp.stopped = true
	}
}

// loadCertificates loads certificates from disk
func (cp *CertificateProvider) loadCertificates() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// Load server certificate and key
	if cp.certFile != "" && cp.keyFile != "" {
		cert, err := stdtls.LoadX509KeyPair(cp.certFile, cp.keyFile)
		if err != nil {
			return fmt.Errorf("failed to load certificate pair: %w", err)
		}
		cp.tlsCert = &cert
		log.Printf("[tls] Loaded server certificate from %s", cp.certFile)
	}

	// Load CA certificate
	if cp.caFile != "" {
		caCert, err := os.ReadFile(cp.caFile)
		if err != nil {
			return fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return fmt.Errorf("failed to parse CA certificate")
		}
		cp.caCertPool = caCertPool
		log.Printf("[tls] Loaded CA certificate from %s", cp.caFile)
	}

	return nil
}

// watchCertificates periodically checks for certificate file changes
func (cp *CertificateProvider) watchCertificates() {
	ticker := time.NewTicker(cp.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cp.stopCh:
			return
		case <-ticker.C:
			if err := cp.checkAndReloadCertificates(); err != nil {
				log.Printf("[tls] Error checking certificates: %v", err)
			}
		}
	}
}

// checkAndReloadCertificates checks if certificates have changed and reloads them
func (cp *CertificateProvider) checkAndReloadCertificates() error {
	files := map[string]string{
		"cert": cp.certFile,
		"key":  cp.keyFile,
		"ca":   cp.caFile,
	}

	changed := false
	for name, path := range files {
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("failed to stat %s: %w", path, err)
		}

		lastMod := info.ModTime()
		if prevMod, exists := cp.lastModTime[name]; exists && prevMod != lastMod {
			changed = true
			log.Printf("[tls] Certificate file changed: %s", path)
		}
		cp.lastModTime[name] = lastMod
	}

	if changed {
		return cp.loadCertificates()
	}
	return nil
}

// GetServerCredentials returns gRPC server credentials
func (cp *CertificateProvider) GetServerCredentials() (credentials.TransportCredentials, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if cp.tlsCert == nil {
		return nil, fmt.Errorf("server certificate not loaded")
	}

	tlsConfig := &stdtls.Config{
		Certificates: []stdtls.Certificate{*cp.tlsCert},
		ClientAuth:   stdtls.RequireAndVerifyClientCert,
	}

	if cp.caCertPool != nil {
		tlsConfig.ClientCAs = cp.caCertPool
	}

	return credentials.NewTLS(tlsConfig), nil
}

// GetClientCredentials returns gRPC client credentials
func (cp *CertificateProvider) GetClientCredentials() (credentials.TransportCredentials, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	tlsConfig := &stdtls.Config{
		InsecureSkipVerify: false,
	}

	// Add client certificate if available
	if cp.tlsCert != nil {
		tlsConfig.Certificates = []stdtls.Certificate{*cp.tlsCert}
	}

	// Add CA certificate for server verification
	if cp.caCertPool != nil {
		tlsConfig.RootCAs = cp.caCertPool
	}

	return credentials.NewTLS(tlsConfig), nil
}

