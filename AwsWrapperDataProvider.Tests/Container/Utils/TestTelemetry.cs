// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Amazon.XRay.Recorder.Core;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;

namespace AwsWrapperDataProvider.Tests.Container.Utils;

/// <summary>
/// Container-side telemetry bootstrap for integration tests. This
/// class wires the AWS X-Ray SDK and the OpenTelemetry SDK so the spans
/// and metrics emitted by the wrapper actually flow to the
/// <c>otlp-daemon</c> Docker container spun up by the host-side
/// <c>TestEnvironmentConfig</c>.
/// </summary>
internal static class TestTelemetry
{
    /// <summary>
    /// Resource <c>service.name</c> attribute value for metrics emitted
    /// from the integration-test process.
    /// </summary>
    private const string ServiceName = "AwsAdvancedDotnetDataProviderWrapperIntegrationTests";

    /// <summary>
    /// Meter name registered by <c>OtlpTelemetryFactory</c> in the wrapper.
    /// </summary>
    private const string InstrumentationName = "aws-advanced-dotnet-wrapper";

    /// <summary>
    /// How often the metrics exporter pushes accumulated metrics to the
    /// OTLP collector.
    /// </summary>
    private const int MetricExportIntervalMs = 5_000;

    private static readonly object SyncRoot = new();
    private static bool initialized;
    private static MeterProvider? meterProvider;

    /// <summary>
    /// Bootstraps the AWS X-Ray SDK (when traces are enabled) and the
    /// OpenTelemetry MeterProvider (when metrics are enabled) so the
    /// wrapper's emitted spans and metrics reach the test-environment
    /// telemetry containers. Idempotent: subsequent calls in the same
    /// process are no-ops.
    /// </summary>
    /// <param name="info">The deserialized test-environment description
    /// produced by the host-side runner. <see cref="TestEnvironmentInfo.TracesTelemetryInfo"/>
    /// and <see cref="TestEnvironmentInfo.MetricsTelemetryInfo"/> carry the
    /// daemon endpoints; <see cref="TestEnvironmentRequest.Features"/>
    /// gates which backends are wired up.</param>
    public static void Setup(TestEnvironmentInfo info)
    {
        lock (SyncRoot)
        {
            if (initialized)
            {
                return;
            }

            // Register the shutdown hook before any provider build so that
            // even a partial bootstrap (e.g., metrics threw) still gets
            // flushed at process exit.
            AppDomain.CurrentDomain.ProcessExit += OnProcessExit;

            HashSet<TestEnvironmentFeatures> features = info.Request!.Features;
            bool tracesEnabled = features.Contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED);
            bool metricsEnabled = features.Contains(TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED);

            if (!tracesEnabled && !metricsEnabled)
            {
                Console.WriteLine(
                    $"{Timestamp()} TestTelemetry: neither TELEMETRY_TRACES_ENABLED nor TELEMETRY_METRICS_ENABLED "
                    + "is set; skipping SDK bootstrap.");
                initialized = true;
                return;
            }

            if (tracesEnabled)
            {
                ConfigureXRayDaemon(info);
            }

            if (metricsEnabled)
            {
                meterProvider = BuildMeterProvider(info);
            }

            initialized = true;
            Console.WriteLine(
                $"{Timestamp()} TestTelemetry: setup complete. "
                + $"traces={(tracesEnabled ? "XRAY" : "off")}, "
                + $"metrics={(metricsEnabled ? "OTLP" : "off")}.");
        }
    }

    /// <summary>
    /// Flushes and disposes the meter provider. Safe to call multiple
    /// times; subsequent calls are no-ops. The X-Ray SDK has no equivalent
    /// flush hook because <c>UdpSegmentEmitter</c> ships each segment
    /// synchronously when <c>EndSegment</c> / <c>EndSubsegment</c> runs,
    /// so by the time a test method returns, traces are already on the
    /// wire.
    /// </summary>
    /// <remarks>
    /// Called both from the <see cref="AppDomain.ProcessExit"/> hook
    /// registered in <see cref="Setup"/> and from explicit teardown paths
    /// in dedicated telemetry tests so they can verify that flush
    /// succeeded before the test process tears down.
    /// </remarks>
    public static void Shutdown()
    {
        MeterProvider? localMeter;

        lock (SyncRoot)
        {
            localMeter = meterProvider;
            meterProvider = null;
        }

        if (localMeter == null)
        {
            return;
        }

        // ForceFlush is best-effort; we still want to dispose even if it
        // fails so we don't leak the gRPC channel held by the exporter.
        TryFlush(() => localMeter.ForceFlush(MetricExportIntervalMs), "meter provider");
        TryDispose(localMeter, "meter provider");
    }

    private static MeterProvider BuildMeterProvider(TestEnvironmentInfo info)
    {
        TestTelemetryInfo metricsInfo = RequireEndpoint(
            info.MetricsTelemetryInfo,
            "MetricsTelemetryInfo");

        return Sdk.CreateMeterProviderBuilder()
            .AddMeter(InstrumentationName)
            .ConfigureResource(r => r.AddService(ServiceName))
            .AddOtlpExporter(
                (exporterOpt, readerOpt) =>
                {
                    exporterOpt.Endpoint = BuildOtlpEndpoint(metricsInfo);
                    exporterOpt.Protocol = OtlpExportProtocol.Grpc;
                    readerOpt.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds =
                        MetricExportIntervalMs;
                })
            .Build()!;
    }

    private static void ConfigureXRayDaemon(TestEnvironmentInfo info)
    {
        TestTelemetryInfo tracesInfo = RequireEndpoint(
            info.TracesTelemetryInfo,
            "TracesTelemetryInfo");

        // The .NET X-Ray SDK reads AWS_XRAY_DAEMON_ADDRESS first, falling
        // back to whatever was passed to SetDaemonAddress. We unconditionally
        // set both so an env var leaking in from the host runner cannot
        // override the address baked into the test container.
        string daemonAddress = $"{tracesInfo.Endpoint}:{tracesInfo.EndpointPort}";
        Environment.SetEnvironmentVariable("AWS_XRAY_DAEMON_ADDRESS", daemonAddress);
        AWSXRayRecorder.Instance.SetDaemonAddress(daemonAddress);
    }

    private static Uri BuildOtlpEndpoint(TestTelemetryInfo telemetryInfo)
    {
        // The OTLP gRPC exporter expects an http:// (or https://) scheme.
        // The test collector runs without TLS, so we always use plain http.
        return new Uri($"http://{telemetryInfo.Endpoint}:{telemetryInfo.EndpointPort}");
    }

    private static TestTelemetryInfo RequireEndpoint(TestTelemetryInfo? info, string memberName)
    {
        if (info == null
            || string.IsNullOrEmpty(info.Endpoint)
            || info.EndpointPort == null)
        {
            throw new InvalidOperationException(
                $"Telemetry feature is enabled but TestEnvironmentInfo.{memberName} is missing or incomplete. "
                + "Check that the host-side TestEnvironmentConfig has populated the corresponding "
                + "TestTelemetryInfo before serializing TEST_ENV_INFO_JSON.");
        }

        return info;
    }

    private static void OnProcessExit(object? sender, EventArgs e) => Shutdown();

    private static void TryFlush(Func<bool> flush, string label)
    {
        try
        {
            if (!flush())
            {
                Console.WriteLine($"{Timestamp()} TestTelemetry: {label} ForceFlush returned false.");
            }
        }
        catch (Exception ex)
        {
            // A flush failure is informational here — we still want to fall
            // through to Dispose so we release the exporter socket.
            Console.WriteLine($"{Timestamp()} TestTelemetry: {label} ForceFlush threw: {ex.Message}");
        }
    }

    private static void TryDispose(IDisposable disposable, string label)
    {
        try
        {
            disposable.Dispose();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"{Timestamp()} TestTelemetry: {label} Dispose threw: {ex.Message}");
        }
    }

    private static string Timestamp() => DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
}
