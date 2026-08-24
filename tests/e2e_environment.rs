//! Opt-in dependency checks run by the Docker Compose E2E runner.
//!
//! These are deliberately transport-level checks for the fault scripts. The
//! application-level adapter and archival scenarios live in `tests/e2e.rs`.

use std::{
    env,
    io::{Read, Write},
    net::{Shutdown, TcpStream},
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(3);

fn enabled() -> bool {
    env::var("PULPITUM_E2E").as_deref() == Ok("1")
}

fn expected(service: &str) -> bool {
    env::var(format!("E2E_EXPECT_{}", service)).unwrap_or_else(|_| "up".to_owned()) == "up"
}

fn cockroach_available() -> bool {
    let host = env::var("E2E_COCKROACH_HOST").expect("E2E_COCKROACH_HOST must be set");
    let port = env::var("E2E_COCKROACH_PORT").expect("E2E_COCKROACH_PORT must be set");
    let mut stream = match TcpStream::connect((host.as_str(), port.parse::<u16>().unwrap())) {
        Ok(stream) => stream,
        Err(_) => return false,
    };
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(TIMEOUT)).unwrap();

    // PostgreSQL SSLRequest. CockroachDB responds with one byte (S or N), which
    // proves traffic passed through the proxy rather than merely connecting to it.
    if stream.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).is_err() {
        return false;
    }
    let mut response = [0_u8; 1];
    stream.read_exact(&mut response).is_ok()
}

fn s3_available() -> bool {
    let endpoint = env::var("E2E_S3_ENDPOINT").expect("E2E_S3_ENDPOINT must be set");
    let authority = endpoint
        .strip_prefix("http://")
        .expect("only http S3 endpoints are supported by this probe");
    let mut stream = match TcpStream::connect(authority) {
        Ok(stream) => stream,
        Err(_) => return false,
    };
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(TIMEOUT)).unwrap();
    let request = format!(
        "GET /minio/health/ready HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n"
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    let _ = stream.shutdown(Shutdown::Both);
    response.starts_with("HTTP/1.1 200")
}

#[test]
fn cockroach_client_connectivity_matches_fault_expectation() {
    if !enabled() {
        return;
    }
    assert_eq!(
        cockroach_available(),
        expected("COCKROACH"),
        "CockroachDB client connectivity did not match E2E_EXPECT_COCKROACH"
    );
}

#[test]
fn s3_connectivity_matches_fault_expectation() {
    if !enabled() {
        return;
    }
    assert_eq!(
        s3_available(),
        expected("S3"),
        "S3/MinIO connectivity did not match E2E_EXPECT_S3"
    );
}
