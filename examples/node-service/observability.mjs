// Install in the Node service:
// npm i @opentelemetry/api @opentelemetry/sdk-node
//       @opentelemetry/exporter-trace-otlp-http
//       @opentelemetry/exporter-metrics-otlp-http
//       @opentelemetry/resources @opentelemetry/semantic-conventions
// Import this module before importing Express/Fastify/database clients.
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions';

const endpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? 'http://127.0.0.1:4318';

const sdk = new NodeSDK({
  resource: resourceFromAttributes({
    [ATTR_SERVICE_NAME]: process.env.OTEL_SERVICE_NAME ?? 'messages-api',
    [ATTR_SERVICE_VERSION]: process.env.APP_VERSION ?? 'unknown',
    'deployment.environment.name': process.env.DEPLOYMENT_ENVIRONMENT ?? 'development',
  }),
  traceExporter: new OTLPTraceExporter({ url: `${endpoint}/v1/traces` }),
  metricReader: new PeriodicExportingMetricReader({
    exporter: new OTLPMetricExporter({ url: `${endpoint}/v1/metrics` }),
    exportIntervalMillis: 10_000,
  }),
});

await sdk.start();
for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => sdk.shutdown().finally(() => process.exit(0)));
}
