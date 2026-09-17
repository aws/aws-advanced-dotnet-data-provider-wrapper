# Telemetry X-Ray Example

End-to-end sample showing how to ship trace spans from the AWS Advanced
.NET Data Provider Wrapper directly to AWS X-Ray, without going through
the OpenTelemetry SDK. The accompanying Docker setup runs a local X-Ray
daemon that forwards segments to the AWS X-Ray service.

The X-Ray backend lives in the
[`AWS.AdvancedDotnetDataProviderWrapper.Telemetry.XRay`](https://www.nuget.org/packages/AWS.AdvancedDotnetDataProviderWrapper.Telemetry.XRay)
NuGet package. A single `XRayTelemetryLoader.Load()` call at startup
registers the backend, and from then on
`TelemetryTracesBackend=XRAY` on the connection string routes wrapper
spans through the in-process X-Ray SDK
(`AWSXRayRecorder.Core`).

For more context on the wrapper's telemetry feature, see the
[Telemetry guide](../../using-the-dotnet-driver/Telemetry.md).

## Configure the connection string

Edit `PGTelemetryXRay.cs` and replace the placeholders in the
`ConnectionString` constant with values for a database you own:

| Placeholder  | Description                              |
|--------------|------------------------------------------|
| `<host>`     | Aurora / RDS endpoint or any reachable PostgreSQL host |
| `<db_name>`  | Database name                            |
| `<user>`     | Database username                        |
| `<password>` | Database password                        |

## Configure the daemon

`docker-compose.yml` runs the daemon and ships segments to AWS X-Ray.
Before running, set the `<region>` placeholder in `docker-compose.yml`
to your target AWS region (for example `us-east-1`).

## Prerequisites

- .NET 10 SDK
- Docker
- AWS credentials in the host shell with permission to write to AWS
  X-Ray (`xray:PutTraceSegments`, `xray:PutTelemetryRecords` — both
  covered by the AWS-managed `AWSXRayDaemonWriteAccess` policy)
- Network access from the host to the database on the configured port

## Run

```bash
# 1. Export AWS credentials so the daemon can sign PutTraceSegments calls.
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...   # only needed for short-lived credentials

# 2. Start the X-Ray daemon.
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
PGTelemetryXRay (this app)
    -> AwsWrapperConnection
        -> wrapper telemetry layer
            -> XRayTelemetryFactory
                -> AWSXRayRecorder.Core (in-process X-Ray SDK)
                    -> UDP :2000 -> X-Ray daemon (Docker)
                        -> HTTPS + SigV4 -> AWS X-Ray
```

## Troubleshooting

- **No spans visible in the X-Ray console.** Check
  `docker logs telemetry-xray-example-daemon --since 1m`. The line you
  want to see is `Successfully sent batch of N segments`. If you see
  `failed to refresh cached credentials`, the AWS env vars never reached
  the container — re-export them in your shell and recreate the
  container with `docker compose down && docker compose up -d`.
- **`ExpiredToken` from the daemon.** Refresh credentials on the host,
  re-export, and recreate the container.
- **Sample exits before the daemon flushes.** Increase the
  `Task.Delay(2s)` at the end of `Main` if your network adds noticeable
  latency.
