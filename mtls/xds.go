package mtls

import (
	"log"
	"os"
	"path/filepath"

	tlsv1 "github.com/dubbo-kubernetes/xds-api/extensions/transport_sockets/tls/v1"
)

// XDSCertificateManager manages certificates from xDS configuration
type XDSCertificateManager struct {
	certDir string
}

// NewXDSCertificateManager creates a new xDS certificate manager
func NewXDSCertificateManager(certDir string) *XDSCertificateManager {
	if certDir == "" {
		certDir = "/etc/dubbo/certs"
	}
	return &XDSCertificateManager{certDir: certDir}
}

// ResolveCertFiles resolves cert/key/ca file paths from a CommonTlsContext.
func (m *XDSCertificateManager) ResolveCertFiles(ctx *tlsv1.CommonTlsContext) (certFile, keyFile, caFile string) {
	if ctx == nil {
		return
	}
	if ctx.TlsCertificateCertificateProviderInstance != nil {
		inst := ctx.TlsCertificateCertificateProviderInstance
		certFile = m.resolvePath(inst.InstanceName, inst.CertificateName, "cert")
		keyFile = m.resolvePath(inst.InstanceName, inst.CertificateName, "key")
	}
	if vc := ctx.GetCombinedValidationContext(); vc != nil {
		if vc.ValidationContextCertificateProviderInstance != nil {
			inst := vc.ValidationContextCertificateProviderInstance
			caFile = m.resolvePath(inst.InstanceName, inst.CertificateName, "ca")
		}
	}
	return
}

func (m *XDSCertificateManager) resolvePath(instanceName, certificateName, certType string) string {
	envVar := "DUBBO_CERT_" + instanceName + "_" + certificateName + "_" + certType
	if path := os.Getenv(envVar); path != "" {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	filename := certificateName + "." + certType
	for _, dir := range []string{
		m.certDir,
		"/etc/dubbo/proxy/certs",
		"/var/run/secrets/workload-spiffe-credentials",
	} {
		path := filepath.Join(dir, filename)
		if _, err := os.Stat(path); err == nil {
			log.Printf("[mtls] resolved %s/%s/%s -> %s", instanceName, certificateName, certType, path)
			return path
		}
	}
	return ""
}
