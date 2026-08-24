use async_trait::async_trait;
use pgwire::{
    api::{
        ClientInfo,
        auth::{
            AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
            noop::NoopStartupHandler,
            sasl::{SASLAuthStartupHandler, scram::ScramAuth},
        },
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{PgWireBackendMessage, PgWireFrontendMessage},
    tokio::TlsAcceptor,
};
use rustls::{ServerConfig, pki_types::CertificateDer};
use rustls_pki_types::{PrivateKeyDer, pem::PemObject};
use std::{env, ffi::OsString, fmt::Debug, fs, io, sync::Arc};
use thiserror::Error;
use uuid::Uuid;

const CERT_PATH_ENV: &str = "PULPITUM_SQL_TLS_CERT_PATH";
const KEY_PATH_ENV: &str = "PULPITUM_SQL_TLS_KEY_PATH";
const PASSWORD_FILE_ENV: &str = "PULPITUM_SQL_PASSWORD_FILE";
const USER_ENV: &str = "PULPITUM_SQL_USER";
const DATABASE_ENV: &str = "PULPITUM_SQL_DATABASE";
const DEFAULT_USER: &str = "pulpitum";
const DEFAULT_DATABASE: &str = "pulpitum";
const SCRAM_ITERATIONS: usize = 4096;

#[derive(Clone)]
pub(crate) enum SidecarSecurity {
    Insecure,
    Secure(Arc<SecureSecurity>),
}

pub(crate) struct SecureSecurity {
    tls_acceptor: TlsAcceptor,
    certificate_pem: Arc<[u8]>,
    credentials: Arc<CredentialSource>,
}

impl SidecarSecurity {
    pub(crate) fn from_environment() -> Result<Self, SecurityConfigError> {
        Self::from_options(
            env::var_os(CERT_PATH_ENV),
            env::var_os(KEY_PATH_ENV),
            env::var_os(PASSWORD_FILE_ENV),
            env::var_os(USER_ENV),
            env::var_os(DATABASE_ENV),
        )
    }

    fn from_options(
        certificate_path: Option<OsString>,
        key_path: Option<OsString>,
        password_path: Option<OsString>,
        user: Option<OsString>,
        database: Option<OsString>,
    ) -> Result<Self, SecurityConfigError> {
        let (certificate_path, key_path, password_path) =
            match (certificate_path, key_path, password_path) {
                (None, None, None) => return Ok(Self::Insecure),
                (Some(certificate), Some(key), Some(password)) => (certificate, key, password),
                _ => return Err(SecurityConfigError::IncompleteSecureConfiguration),
            };

        let user = configured_name(user, DEFAULT_USER, USER_ENV)?;
        let database = configured_name(database, DEFAULT_DATABASE, DATABASE_ENV)?;
        let certificate_pem = fs::read(certificate_path)
            .map_err(|source| SecurityConfigError::ReadCertificate { source })?;
        let key_pem =
            fs::read(key_path).map_err(|source| SecurityConfigError::ReadPrivateKey { source })?;
        let password_bytes = fs::read(password_path)
            .map_err(|source| SecurityConfigError::ReadPassword { source })?;

        Self::secure_from_bytes(user, database, certificate_pem, key_pem, password_bytes)
    }

    fn secure_from_bytes(
        user: String,
        database: String,
        certificate_pem: Vec<u8>,
        key_pem: Vec<u8>,
        password_bytes: Vec<u8>,
    ) -> Result<Self, SecurityConfigError> {
        let password = parse_password_file(password_bytes)?;
        let certificates = CertificateDer::pem_slice_iter(&certificate_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| SecurityConfigError::InvalidCertificate)?;
        if certificates.is_empty() {
            return Err(SecurityConfigError::InvalidCertificate);
        }
        let private_key = PrivateKeyDer::from_pem_slice(&key_pem)
            .map_err(|_| SecurityConfigError::InvalidPrivateKey)?;
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let builder = ServerConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .map_err(|_| SecurityConfigError::InvalidCryptoProvider)?
            .with_no_client_auth();
        let mut tls_config = builder
            .with_single_cert(certificates, private_key)
            .map_err(|_| SecurityConfigError::InvalidTlsIdentity)?;
        tls_config.alpn_protocols = vec![b"postgresql".to_vec()];

        let credentials = Arc::new(CredentialSource::new(user, database, &password));
        let mut channel_binding = ScramAuth::new(credentials.clone());
        channel_binding
            .configure_certificate(&certificate_pem)
            .map_err(|_| SecurityConfigError::InvalidChannelBindingCertificate)?;
        Ok(Self::Secure(Arc::new(SecureSecurity {
            tls_acceptor: TlsAcceptor::from(Arc::new(tls_config)),
            certificate_pem: certificate_pem.into(),
            credentials,
        })))
    }

    pub(crate) fn is_secure(&self) -> bool {
        matches!(self, Self::Secure(_))
    }

    pub(crate) fn tls_acceptor(&self) -> Option<TlsAcceptor> {
        match self {
            Self::Insecure => None,
            Self::Secure(security) => Some(security.tls_acceptor.clone()),
        }
    }

    pub(crate) fn startup_handler(&self) -> SecurityStartupHandler {
        match self {
            Self::Insecure => SecurityStartupHandler::Insecure(InsecureStartupHandler),
            Self::Secure(security) => {
                let mut scram = ScramAuth::new(security.credentials.clone());
                scram.set_iterations(SCRAM_ITERATIONS);
                scram
                    .configure_certificate(&security.certificate_pem)
                    .expect("the certificate was validated while loading secure configuration");
                let handler = SASLAuthStartupHandler::new(Arc::new(
                    DefaultServerParameterProvider::default(),
                ))
                .with_scram(scram);
                SecurityStartupHandler::Secure(handler)
            }
        }
    }
}

fn configured_name(
    value: Option<OsString>,
    default: &str,
    variable: &'static str,
) -> Result<String, SecurityConfigError> {
    let value = match value {
        Some(value) => value
            .into_string()
            .map_err(|_| SecurityConfigError::InvalidName { variable })?,
        None => default.to_owned(),
    };
    if value.is_empty() {
        return Err(SecurityConfigError::InvalidName { variable });
    }
    Ok(value)
}

fn parse_password_file(mut bytes: Vec<u8>) -> Result<String, SecurityConfigError> {
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    if bytes.is_empty() || bytes.contains(&b'\n') || bytes.contains(&b'\r') || bytes.contains(&0) {
        return Err(SecurityConfigError::InvalidPasswordFile);
    }
    String::from_utf8(bytes).map_err(|_| SecurityConfigError::InvalidPasswordFile)
}

#[derive(Debug)]
struct CredentialSource {
    user: String,
    database: String,
    salt: Vec<u8>,
    salted_password: Vec<u8>,
    decoy_salt: Vec<u8>,
    decoy_salted_password: Vec<u8>,
}

impl CredentialSource {
    fn new(user: String, database: String, password: &str) -> Self {
        let salt = Uuid::new_v4().as_bytes().to_vec();
        let decoy_salt = Uuid::new_v4().as_bytes().to_vec();
        let decoy_password = Uuid::new_v4().to_string();
        Self {
            user,
            database,
            salted_password: pgwire::api::auth::sasl::scram::gen_salted_password(
                password,
                &salt,
                SCRAM_ITERATIONS,
            ),
            salt,
            decoy_salted_password: pgwire::api::auth::sasl::scram::gen_salted_password(
                &decoy_password,
                &decoy_salt,
                SCRAM_ITERATIONS,
            ),
            decoy_salt,
        }
    }
}

#[async_trait]
impl AuthSource for CredentialSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        if login.user() == Some(self.user.as_str())
            && login.database() == Some(self.database.as_str())
        {
            Ok(Password::new(
                Some(self.salt.clone()),
                self.salted_password.clone(),
            ))
        } else {
            Ok(Password::new(
                Some(self.decoy_salt.clone()),
                self.decoy_salted_password.clone(),
            ))
        }
    }
}

pub(crate) enum SecurityStartupHandler {
    Insecure(InsecureStartupHandler),
    Secure(SASLAuthStartupHandler<DefaultServerParameterProvider>),
}

#[async_trait]
impl StartupHandler for SecurityStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + futures::Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as futures::Sink<PgWireBackendMessage>>::Error>,
    {
        match self {
            Self::Insecure(handler) => handler.on_startup(client, message).await,
            Self::Secure(handler) => {
                if matches!(message, PgWireFrontendMessage::Startup(_)) && !client.is_secure() {
                    return Err(tls_required_error());
                }
                handler.on_startup(client, message).await
            }
        }
    }
}

pub(crate) struct InsecureStartupHandler;

#[async_trait]
impl NoopStartupHandler for InsecureStartupHandler {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + futures::Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        tracing::debug!(peer = %client.socket_addr(), "SQL client connected without transport security");
        Ok(())
    }
}

fn tls_required_error() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".to_owned(),
        "28000".to_owned(),
        "TLS is required".to_owned(),
    )))
}

#[derive(Debug, Error)]
pub(crate) enum SecurityConfigError {
    #[error("{CERT_PATH_ENV}, {KEY_PATH_ENV}, and {PASSWORD_FILE_ENV} must be configured together")]
    IncompleteSecureConfiguration,
    #[error("{variable} must be non-empty UTF-8")]
    InvalidName { variable: &'static str },
    #[error("could not read the SQL sidecar TLS certificate")]
    ReadCertificate { source: io::Error },
    #[error("could not read the SQL sidecar TLS private key")]
    ReadPrivateKey { source: io::Error },
    #[error("could not read the SQL sidecar password file")]
    ReadPassword { source: io::Error },
    #[error("the SQL sidecar TLS certificate is empty or invalid")]
    InvalidCertificate,
    #[error("the SQL sidecar TLS private key is empty or invalid")]
    InvalidPrivateKey,
    #[error("the SQL sidecar TLS certificate and private key do not form a valid identity")]
    InvalidTlsIdentity,
    #[error("the SQL sidecar TLS certificate cannot be used for SCRAM channel binding")]
    InvalidChannelBindingCertificate,
    #[error("the SQL sidecar TLS cryptographic provider could not be configured")]
    InvalidCryptoProvider,
    #[error("the SQL sidecar password file must contain one non-empty UTF-8 line")]
    InvalidPasswordFile,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgwire::{api::PgWireServerHandlers, tokio::process_socket};
    use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
    use sha2::{Digest, Sha256};
    use std::{
        future::Future,
        io,
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::{
        io::{AsyncRead, AsyncWrite, ReadBuf},
        net::TcpListener,
    };
    use tokio_postgres::{
        Config, NoTls,
        config::{ChannelBinding, SslMode},
        error::SqlState,
        tls::{ChannelBinding as TlsChannelBinding, MakeTlsConnect, TlsConnect, TlsStream},
    };
    use tokio_rustls::{TlsConnector, client::TlsStream as RustlsTlsStream};

    const TEST_CERTIFICATE: &[u8] = br#"-----BEGIN CERTIFICATE-----
MIIC/DCCAeSgAwIBAgIJAIaaUYjfPIwoMA0GCSqGSIb3DQEBCwUAMBQxEjAQBgNV
BAMMCWxvY2FsaG9zdDAeFw0yNjA4MjQyMDUyMzNaFw0zNjA4MjEyMDUyMzNaMBQx
EjAQBgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
ggEBAM6m5RQWLDZxIKHFDAoWh2yu6brv707tHF1HsZHVMnRVcj3xycCK92xxM8uc
IQRkoY36/Oy+yPfUvyhrPxQYSwJRj+i43Y0tKrRWQhRp6zer2SPYNvlpMqHMRXCd
GNMFJhfB9kCiX+MkBJQh322N/VGsLIGs1CFhaNIMlBJlwEY5FqEMekZEz4TCd9Ic
NV/iBJn8GDv6nwFTk/MgSMASeyj724pR2qX6d2v94f4fR8BW0ISZAl2hWG+PuD7/
U4/UPipWvZl9Fv9qu/ZOUXjrqGzcaMHw+84WyjBATXj487aQqvPa39Jq8t9vN58V
RDh6iKsIff/tKJtajxkwmwokFN0CAwEAAaNRME8wGgYDVR0RBBMwEYIJbG9jYWxo
b3N0hwR/AAABMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgWgMBMGA1UdJQQM
MAoGCCsGAQUFBwMBMA0GCSqGSIb3DQEBCwUAA4IBAQC4LgNbQWRj63s9kIXn8RGk
S6jK107AXfK+6U5eLGAIhJwWfMgDiPxajQHfcF4bIBwV5Nh7/f04Re1Kji6yg1nE
fTfJAYu9LtFRjuyzBEis00glYmorlpRyQUtvG7aN0ORo118TmPJOHb0s+HrxXxWE
ehCxBc/72YIwAu/8xqAGDlTMCS3/MnCMpOXMlGk1uYYNKQlZBjVCkOVD/rVjagXV
b1UOBc8f/mhUGb6krdteWIvUnHjNTmLjh5pHHNVxt+H7zASRnCEKgWLMntD/BvOj
37UPCCzEXkdGXBGqxO+UvclG5a4kpRxfZCZUdS3uHNoKUjNkRzbMa50+Z+5eJzCh
-----END CERTIFICATE-----
"#;
    const TEST_PRIVATE_KEY: &[u8] = br#"-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEAzqblFBYsNnEgocUMChaHbK7puu/vTu0cXUexkdUydFVyPfHJ
wIr3bHEzy5whBGShjfr87L7I99S/KGs/FBhLAlGP6LjdjS0qtFZCFGnrN6vZI9g2
+WkyocxFcJ0Y0wUmF8H2QKJf4yQElCHfbY39UawsgazUIWFo0gyUEmXARjkWoQx6
RkTPhMJ30hw1X+IEmfwYO/qfAVOT8yBIwBJ7KPvbilHapfp3a/3h/h9HwFbQhJkC
XaFYb4+4Pv9Tj9Q+Kla9mX0W/2q79k5ReOuobNxowfD7zhbKMEBNePjztpCq89rf
0mry3283nxVEOHqIqwh9/+0om1qPGTCbCiQU3QIDAQABAoIBAFyIuxctfoq6SWRm
uadiwy1VfW+ptLzgy8yxJ8AneTpCcK9wL2k6UOSMJCdOODKhZP4Qn2TbYV4oM5jD
vTEgV6YoI4qQDRUEXpT18wz1CNCa8NZuIN+5zWRJ9eYhUlZbfd0xizUSAGHTZQF3
0XZbGE2UDTHb0/lGhwtXeo5qZZiLdZRe3bSGohXI5G1xBdLGIdp4A1EQlpfX6b4E
TWj2J74qA/ZJa9M7Mj2amWqDVtniO++R8OtM/sdnJca6qBXQ8aL1ehFX+U3GAmI4
wdzT/23z0nD9GPaxgF06gVhNCbc7Aylu6hmvJzkHv8CSlVBb0wr5odO2H5u9JiW7
JPyHBnkCgYEA7QB9O0oZrmAT4k0Dpn9ItGQGpgdcbKUG+u5NIpb0Xx6YolljNeLj
CIpoZd4cg6TBxbf6njFlny9DyGtJjrrET6lQpx04uVbtKCs8C35EcmuoEEK84kNG
tg+Pipe2wxtxg4c23cE72zpdroPebR5YgdB5vPP5MBzpxS9iqMtIWQ8CgYEA3zeY
LaFschx5LUnePTojKBy/vv91LokvmrqxKtNMr9lProCQ1H3M8GIkxK1uADym+rKJ
WI9H3bZBqu4IpwvNTmk72b7GUq+q0Oh6InpiSwLnQuhpu3hDODyUpiwsNNxVD9UG
hJm2nOLSi4dUlqCNXbKvcVq+gGPPUU/XPUDLe1MCgYEA3WeflkvbQfOvn7Giv2AZ
Y6wuKdymkzh4FOOaW735/QJwRPqMnEKhJdFnRgMBUFoSS8tb7XzoGpXlFM5loVkJ
HAJovjWmUD7MvsHlDjefaeT41HgETLvcyyguSKMCsbJpkR44O2HRsTQNYIMAv5+h
v2Qq1kJ1gGUCXput51JA/DECgYEAunFhNpviTN3jiSRt8I4i11pL/mi5pAkKLh9J
5A9hum+00niogBQjnZUcSPrjKmd+wV9mwQXMbS/SYcc4iu6cqaXUS+fBF0eLUbsc
aLf4adce+w/NYLzuyIYxUysKMYznr7WrIA6ULS531ftPeBDagyzAxdmZzPuRKsWZ
bpw0WekCgYEAxMxqPFB1fdZmfJUGgzCMrzmHjGZXyrYCT9VolOTTODOw1+Kq60uX
mKFGciP8YfwzFsKgrFw6hrwdUf/EiDWudO6gnFiu0+nGmTq0htSRH1tsZlIzmahg
HS9FDnLR4PBr4/i1VyjbHmp4O1fxV1oAc2D/iUdEE/m2B9JF9I9BG4o=
-----END RSA PRIVATE KEY-----
"#;

    struct TestFactory {
        security: SidecarSecurity,
    }

    #[derive(Clone)]
    struct ChannelBindingTlsConnector {
        config: Arc<ClientConfig>,
        binding: Vec<u8>,
    }

    struct ChannelBindingTlsConnect {
        connector: TlsConnector,
        server_name: ServerName<'static>,
        binding: Vec<u8>,
    }

    struct ChannelBindingTlsStream<S> {
        stream: RustlsTlsStream<S>,
        binding: Vec<u8>,
    }

    impl<S> MakeTlsConnect<S> for ChannelBindingTlsConnector
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        type Stream = ChannelBindingTlsStream<S>;
        type TlsConnect = ChannelBindingTlsConnect;
        type Error = io::Error;

        fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error> {
            let server_name = ServerName::try_from(domain)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid TLS hostname"))?
                .to_owned();
            Ok(ChannelBindingTlsConnect {
                connector: TlsConnector::from(self.config.clone()),
                server_name,
                binding: self.binding.clone(),
            })
        }
    }

    impl<S> TlsConnect<S> for ChannelBindingTlsConnect
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        type Stream = ChannelBindingTlsStream<S>;
        type Error = io::Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send>>;

        fn connect(self, stream: S) -> Self::Future {
            Box::pin(async move {
                let stream = self.connector.connect(self.server_name, stream).await?;
                Ok(ChannelBindingTlsStream {
                    stream,
                    binding: self.binding,
                })
            })
        }
    }

    impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for ChannelBindingTlsStream<S> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_read(context, buffer)
        }
    }

    impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for ChannelBindingTlsStream<S> {
        fn poll_write(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.stream).poll_write(context, buffer)
        }

        fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_flush(context)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_shutdown(context)
        }
    }

    impl<S: AsyncRead + AsyncWrite + Unpin> TlsStream for ChannelBindingTlsStream<S> {
        fn channel_binding(&self) -> TlsChannelBinding {
            TlsChannelBinding::tls_server_end_point(self.binding.clone())
        }
    }

    impl PgWireServerHandlers for TestFactory {
        fn startup_handler(&self) -> Arc<impl StartupHandler> {
            Arc::new(self.security.startup_handler())
        }
    }

    async fn serve_once(security: SidecarSecurity) -> (u16, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let tls_acceptor = security.tls_acceptor();
        let factory = Arc::new(TestFactory { security });
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = process_socket(socket, tls_acceptor, factory).await;
        });
        (port, task)
    }

    fn secure_test_config() -> SidecarSecurity {
        SidecarSecurity::secure_from_bytes(
            "app".to_owned(),
            "records".to_owned(),
            TEST_CERTIFICATE.to_vec(),
            TEST_PRIVATE_KEY.to_vec(),
            b"correct horse battery staple\n".to_vec(),
        )
        .unwrap()
    }

    fn client_config(port: u16, user: &str, database: &str, password: &str) -> Config {
        let mut config = Config::new();
        config
            .host("localhost")
            .port(port)
            .user(user)
            .dbname(database)
            .password(password);
        config
    }

    fn tls_connector() -> ChannelBindingTlsConnector {
        let certificate = CertificateDer::from_pem_slice(TEST_CERTIFICATE).unwrap();
        let binding = Sha256::digest(certificate.as_ref()).to_vec();
        let mut roots = RootCertStore::empty();
        roots.add(certificate).unwrap();
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let config = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        ChannelBindingTlsConnector {
            config: Arc::new(config),
            binding,
        }
    }

    async fn secure_connect(
        port: u16,
        user: &str,
        database: &str,
        password: &str,
    ) -> Result<(), tokio_postgres::Error> {
        let mut config = client_config(port, user, database, password);
        config
            .ssl_mode(SslMode::Require)
            .channel_binding(ChannelBinding::Require);
        let (client, connection) = config.connect(tls_connector()).await?;
        let connection_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        drop(client);
        let _ = connection_task.await;
        Ok(())
    }

    #[test]
    fn secure_file_configuration_is_all_or_nothing() {
        let cert = Some(OsString::from("cert.pem"));
        let key = Some(OsString::from("key.pem"));
        let password = Some(OsString::from("password"));

        assert!(matches!(
            SidecarSecurity::from_options(None, None, None, None, None),
            Ok(SidecarSecurity::Insecure)
        ));
        for result in [
            SidecarSecurity::from_options(cert.clone(), None, None, None, None),
            SidecarSecurity::from_options(None, key.clone(), None, None, None),
            SidecarSecurity::from_options(None, None, password.clone(), None, None),
            SidecarSecurity::from_options(cert.clone(), key.clone(), None, None, None),
            SidecarSecurity::from_options(cert.clone(), None, password.clone(), None, None),
            SidecarSecurity::from_options(None, key.clone(), password.clone(), None, None),
        ] {
            assert!(matches!(
                result,
                Err(SecurityConfigError::IncompleteSecureConfiguration)
            ));
        }
    }

    #[test]
    fn password_file_accepts_one_optional_line_ending() {
        assert_eq!(parse_password_file(b"secret".to_vec()).unwrap(), "secret");
        assert_eq!(parse_password_file(b"secret\n".to_vec()).unwrap(), "secret");
        assert_eq!(
            parse_password_file(b"secret\r\n".to_vec()).unwrap(),
            "secret"
        );
        for invalid in [b"".as_slice(), b"\n", b"one\ntwo", b"one\r", b"a\0b"] {
            assert!(matches!(
                parse_password_file(invalid.to_vec()),
                Err(SecurityConfigError::InvalidPasswordFile)
            ));
        }
    }

    #[tokio::test]
    async fn insecure_mode_accepts_loopback_protocol_connections_without_a_password() {
        let (port, server) = serve_once(SidecarSecurity::Insecure).await;
        let mut config = client_config(port, "local", "local", "");
        config.ssl_mode(SslMode::Disable);
        let (client, connection) = config.connect(NoTls).await.unwrap();
        let connection_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        drop(client);
        let _ = connection_task.await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn secure_mode_rejects_plaintext_before_authentication() {
        let (port, server) = serve_once(secure_test_config()).await;
        let mut config = client_config(port, "app", "records", "correct horse battery staple");
        config.ssl_mode(SslMode::Disable);
        let error = match config.connect(NoTls).await {
            Ok(_) => panic!("secure mode accepted a plaintext connection"),
            Err(error) => error,
        };
        let database_error = error
            .as_db_error()
            .expect("server returned a PostgreSQL error");
        assert_eq!(
            database_error.code(),
            &SqlState::INVALID_AUTHORIZATION_SPECIFICATION
        );
        assert_eq!(database_error.message(), "TLS is required");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn secure_mode_authenticates_with_scram_plus_channel_binding() {
        let (port, server) = serve_once(secure_test_config()).await;
        secure_connect(port, "app", "records", "correct horse battery staple")
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn wrong_password_user_and_database_are_generic_authentication_failures() {
        for (user, database, password) in [
            ("app", "records", "wrong password"),
            ("other", "records", "correct horse battery staple"),
            ("app", "other", "correct horse battery staple"),
        ] {
            let (port, server) = serve_once(secure_test_config()).await;
            let error = secure_connect(port, user, database, password)
                .await
                .unwrap_err();
            let database_error = error
                .as_db_error()
                .expect("server returned a PostgreSQL error");
            assert_eq!(database_error.code(), &SqlState::INVALID_PASSWORD);
            assert!(
                database_error
                    .message()
                    .starts_with("Password authentication failed for user")
            );
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn wrong_user_or_database_receives_decoy_scram_credentials() {
        let source = CredentialSource::new("app".to_owned(), "records".to_owned(), "secret");
        let expected = source
            .get_password(&LoginInfo::new(
                Some("app"),
                Some("records"),
                "127.0.0.1".to_owned(),
            ))
            .await
            .unwrap();
        for login in [
            LoginInfo::new(Some("other"), Some("records"), "127.0.0.1".to_owned()),
            LoginInfo::new(Some("app"), Some("other"), "127.0.0.1".to_owned()),
            LoginInfo::new(None, None, "127.0.0.1".to_owned()),
        ] {
            let decoy = source.get_password(&login).await.unwrap();
            assert_ne!(decoy.salt(), expected.salt());
            assert_ne!(decoy.password(), expected.password());
        }
    }
}
