package mtls

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	tlsv1 "github.com/dubbo-kubernetes/xds-api/extensions/transport_sockets/tls/v1"
)

// XDSCertificateManager manages certificates from xDS configuration
type XDSCertificateManager struct {
	certProvider *CertificateProvider
	certDir      string
}

// NewXDSCertificateManager creates a new xDS certificate manager
func NewXDSCertificateManager(certDir string) *XDSCertificateManager {
	if certDir == "" {
		certDir = "/etc/dubbo/certs"
	}
	return &XDSCertificateManager{
		certDir: certDir,
	}
}

// ApplyDownstreamTLSContext applies server-side TLS configuration from xDS
func (m *XDSCertificateManager) ApplyDownstreamTLSContext(ctx *tlsv1.DownstreamTlsContext) (*CertificateProvider, error) {
	if ctx == nil {
		return nil, fmt.Errorf("downstream TLS context is nil")
	}

	commonCtx := ctx.CommonTlsContext
	if commonCtx == nil {
		return nil, fmt.Errorf("common TLS context is nil")
	}

	certProvider, err := m.createCertificateProvider(commonCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create certificate provider: %w", err)
	}

	m.certProvider = certProvider
	return certProvider, nil
}

// ApplyUpstreamTLSContext applies client-side TLS configuration from xDS
func (m *XDSCertificateManager) ApplyUpstreamTLSContext(ctx *tlsv1.UpstreamTlsContext) (*CertificateProvider, error) {
	if ctx == nil {
		return nil, fmt.Errorf("upstream TLS context is nil")
	}

	commonCtx := ctx.CommonTlsContext
	if commonCtx == nil {
		return nil, fmt.Errorf("common TLS context is nil")
	}

	certProvider, err := m.createCertificateProvider(commonCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create certificate provider: %w", err)
	}

	m.certProvider = certProvider
	return certProvider, nil
}

// createCertificateProvider creates a certificate provider from common TLS context
func (m *XDSCertificateManager) createCertificateProvider(ctx *tlsv1.CommonTlsContext) (*CertificateProvider, error) {
	certFile := ""
	keyFile := ""
	caFile := ""

	// Extract certificate from certificate provider instance
	if ctx.TlsCertificateCertificateProviderInstance != nil {
		instance := ctx.TlsCertificateCertificateProviderInstance
		certFile = m.resolveCertificatePath(instance.InstanceName, instance.CertificateName, "cert")
		keyFile = m.resolveCertificatePath(instance.InstanceName, instance.CertificateName, "key")
	}

	// Extract CA certificate from validation context (oneof field)
	if validationCtx := ctx.GetCombinedValidationContext(); validationCtx != nil {
		if validationCtx.ValidationContextCertificateProviderInstance != nil {
			instance := validationCtx.ValidationContextCertificateProviderInstance
			caFile = m.resolveCertificatePath(instance.InstanceName, instance.CertificateName, "ca")
		}
	}

	if certFile == "" && keyFile == "" && caFile == "" {
		return nil, fmt.Errorf("no certificates found in TLS context")
	}

	cp := NewCertificateProvider(certFile, keyFile, caFile)
	if err := cp.Start(); err != nil {
		return nil, fmt.Errorf("failed to start certificate provider: %w", err)
	}

	log.Printf("[xds-tls] Created certificate provider: cert=%s key=%s ca=%s", certFile, keyFile, caFile)
	return cp, nil
}

// resolveCertificatePath resolves the actual file path for a certificate
func (m *XDSCertificateManager) resolveCertificatePath(instanceName, certificateName, certType string) string {
	// Try environment variable first
	envVar := fmt.Sprintf("DUBBO_CERT_%s_%s_%s", instanceName, certificateName, certType)
	if path := os.Getenv(envVar); path != "" {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	// Try standard certificate directory
	filename := fmt.Sprintf("%s.%s", certificateName, certType)
	path := filepath.Join(m.certDir, filename)
	if _, err := os.Stat(path); err == nil {
		return path
	}

	// Try /etc/dubbo/proxy/certs
	path = filepath.Join("/etc/dubbo/proxy/certs", filename)
	if _, err := os.Stat(path); err == nil {
		return path
	}

	// Try /var/run/secrets/workload-spiffe-credentials
	path = filepath.Join("/var/run/secrets/workload-spiffe-credentials", filename)
	if _, err := os.Stat(path); err == nil {
		return path
	}

	return ""
}

// GetCertificateProvider returns the current certificate provider
func (m *XDSCertificateManager) GetCertificateProvider() *CertificateProvider {
	return m.certProvider
}

// Stop stops the certificate manager
func (m *XDSCertificateManager) Stop() {
	if m.certProvider != nil {
		m.certProvider.Stop()
	}
}

