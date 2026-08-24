use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use rustls_pki_types::pem::PemObject;
use std::sync::Arc;
use thiserror::Error;
use tokio_postgres_rustls::MakeRustlsConnect;

/// Verified TLS configuration for CockroachDB connections.
///
/// The configured CA roots are used for both certificate-chain and hostname
/// verification. Client authentication can optionally be added for mTLS.
pub struct CockroachTlsConfig {
    roots: RootCertStore,
    client_certificates: Option<Vec<CertificateDer<'static>>>,
    client_key: Option<PrivateKeyDer<'static>>,
}

impl CockroachTlsConfig {
    /// Builds a server-authenticated configuration from a PEM CA bundle.
    pub fn from_ca_pem(ca_pem: &[u8]) -> Result<Self, CockroachTlsConfigError> {
        let certificates = CertificateDer::pem_slice_iter(ca_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| CockroachTlsConfigError::InvalidCaBundle)?;
        if certificates.is_empty() {
            return Err(CockroachTlsConfigError::InvalidCaBundle);
        }

        let mut roots = RootCertStore::empty();
        for certificate in certificates {
            roots
                .add(certificate)
                .map_err(|_| CockroachTlsConfigError::InvalidCaBundle)?;
        }
        Ok(Self {
            roots,
            client_certificates: None,
            client_key: None,
        })
    }

    /// Adds a PEM client certificate chain and private key for mTLS.
    pub fn with_client_auth_pem(
        mut self,
        certificate_chain_pem: &[u8],
        private_key_pem: &[u8],
    ) -> Result<Self, CockroachTlsConfigError> {
        let certificates = CertificateDer::pem_slice_iter(certificate_chain_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| CockroachTlsConfigError::InvalidClientCertificate)?;
        if certificates.is_empty() {
            return Err(CockroachTlsConfigError::InvalidClientCertificate);
        }

        let key = PrivateKeyDer::from_pem_slice(private_key_pem)
            .map_err(|_| CockroachTlsConfigError::InvalidClientKey)?;
        self.client_certificates = Some(certificates);
        self.client_key = Some(key);
        Ok(self)
    }

    pub(crate) fn connector(&self) -> Result<MakeRustlsConnect, CockroachTlsConfigError> {
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let builder = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .map_err(|_| CockroachTlsConfigError::InvalidCryptoProvider)?
            .with_root_certificates(self.roots.clone());
        let config = match (&self.client_certificates, &self.client_key) {
            (Some(certificates), Some(key)) => builder
                .with_client_auth_cert(certificates.clone(), key.clone_key())
                .map_err(|_| CockroachTlsConfigError::InvalidClientIdentity)?,
            (None, None) => builder.with_no_client_auth(),
            _ => return Err(CockroachTlsConfigError::IncompleteClientIdentity),
        };
        Ok(MakeRustlsConnect::new(config))
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CockroachTlsConfigError {
    #[error("CockroachDB CA bundle is empty or invalid")]
    InvalidCaBundle,
    #[error("CockroachDB client certificate chain is empty or invalid")]
    InvalidClientCertificate,
    #[error("CockroachDB client private key is empty or invalid")]
    InvalidClientKey,
    #[error("CockroachDB client certificate and private key must be configured together")]
    IncompleteClientIdentity,
    #[error("CockroachDB client certificate and private key do not form a valid identity")]
    InvalidClientIdentity,
    #[error("CockroachDB TLS cryptographic provider could not be configured")]
    InvalidCryptoProvider,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_and_malformed_ca_bundles() {
        assert!(matches!(
            CockroachTlsConfig::from_ca_pem(b""),
            Err(CockroachTlsConfigError::InvalidCaBundle)
        ));
        assert!(matches!(
            CockroachTlsConfig::from_ca_pem(b"not a certificate"),
            Err(CockroachTlsConfigError::InvalidCaBundle)
        ));
    }
}
