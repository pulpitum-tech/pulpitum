# Security policy

## Supported versions

Security fixes are made on the latest published `0.x` release and the `main` branch. Pulpitum is experimental; review the [production-readiness audit](docs/production-readiness.md) before using it with production data.

## Reporting a vulnerability

Please do **not** open a public issue for a suspected security vulnerability.

Use [GitHub private vulnerability reporting](https://github.com/pulpitum-tech/pulpitum/security/advisories/new) and include:

- affected version or commit;
- a minimal reproduction or clear exploit narrative;
- impact and affected deployment surface;
- suggested mitigation, if known.

We will acknowledge reports within 7 days and coordinate disclosure after a fix or mitigation is available.

## Scope notes

Do not include credentials, private keys, production data, or customer identifiers in a report. The SQL sidecar, S3 credentials, CockroachDB TLS, archival cutover, and object integrity boundaries are all security-sensitive areas.
