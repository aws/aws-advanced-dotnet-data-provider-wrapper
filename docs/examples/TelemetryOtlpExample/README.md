# Telemetry OTLP Example

End-to-end sample showing how to ship trace spans from the AWS Advanced
.NET Data Provider Wrapper through the OpenTelemetry SDK to an
OTLP-compatible backend. As shipped, the accompanying Docker setup runs
the AWS Distro for OpenTelemetry (ADOT) Collector locally and forwards
spans to AWS X-Ray.

The sample uses the OTLP backend that ships in the core wrapper; no
additional NuGet packages from the wrapper itself are required. The user
application configures the OpenTelemetry SDK and an OTLP exporter, and
the wrapper emits spans through `System.Diagnostics.ActivitySource` for
the SDK to pick up.

For more context on the wrapper's telemetry feature, see the
[Telemetry guide](../../using-the-dotnet-driver/Telemetry.md).

## Configure the connection string

Edit `PGTelemetryOtlp.cs` and replace the placeholders in the
`ConnectionString` constant with values for a database you own:

| Placeholder  | Description                              |
|--------------|------------------------------------------|
| `<host>`     | Aurora / RDS endpoint or any reachable PostgreSQL host |
| `<db_name>`  | Database name                            |
| `<user>`     | Database username                        |
| `<password>` | Database password                        |

## Configure the collector

The accompanying `docker-compose.yml` and `adot-config.yaml` ship spans
to AWS X-Ray. Before running, set the `<region>` placeholder in both
files to your target AWS region (for example `us-east-1`).

If you target a different OTLP backend (Honeycomb, Grafana Tempo,
Datadog, etc.), replace the `awsxray` exporter in `adot-config.yaml`
with the exporter for that backend, drop the
`OpenTelemetry.Extensions.AWS` package reference and the
`AddXRayTraceId()` call from `PGTelemetryOtlp.cs`, and skip the AWS
credential setup below.

## Prerequisites

- .NET 10 SDK
- Docker
- AWS credentials in the host shell with permission to write to AWS
  X-Ray (`xray:PutTraceSegments`, `xray:PutTelemetryRecords` — both
  covered by the AWS-managed `AWSXRayDaemonWriteAccess` policy)
- Network access from the host to the database on the configured port

## Run

```bash
# 1. Export AWS credentials so the collector can sign PutTraceSegments calls.
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...   # only needed for short-lived credentials

# 2. Start the ADOT Collector.
docker compose up -d

# 3. Run the sample.
dotnet run
```

Spans appear in the AWS X-Ray console under **CloudWatch → Traces** in
the configured region, typically within ~30 seconds of the sample
completing.

## Stop

```bash
docker compose down
```

## How it works

```
PGTelemetryOtlp (this app)
    -> AwsWrapperConnection
        -> wrapper telemetry layer
            -> System.Diagnostics.ActivitySource("aws-advanced-dotnet-wrapper")
                -> OpenTelemetry SDK (TracerProvider)
                    -> OTLP exporter (HTTP/protobuf, :4318)
                        -> ADOT Collector
                            -> AWS X-Ray
```

## Troubleshooting

- **`docker compose up` fails to find the collector image.** The image
  is hosted on AWS Public ECR and pulls without authentication. If your
  network blocks `public.ecr.aws`, configure egress.
- **No spans visible in the backend.** Check
  `docker logs telemetry-otlp-example-collector --since 1m`. Look for
  the OTLP receiver acknowledging traffic and the `awsxray` exporter
  shipping segments. If nothing arrives at the collector, confirm the
  `AddSource(...)` argument in `PGTelemetryOtlp.cs` matches
  `OtlpTelemetryFactory.InstrumentationName`
  (`aws-advanced-dotnet-wrapper`).
- **`failed to send segments` from `awsxrayexporter`.** Either
  credentials are missing / expired, or trace IDs are not X-Ray-shaped.
  Re-export credentials and recreate the container with
  `docker compose down && docker compose up -d`. Confirm
  `AddXRayTraceId()` is on the `TracerProviderBuilder`.
