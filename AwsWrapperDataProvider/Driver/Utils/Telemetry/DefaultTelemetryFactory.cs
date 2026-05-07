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

using System.Collections.Concurrent;

namespace AwsWrapperDataProvider.Driver.Utils.Telemetry;

/// <summary>
/// Routing <see cref="ITelemetryFactory"/> that reads the telemetry-related
/// connection properties (<c>EnableTelemetry</c> and delegates trace and metric operations
/// to the appropriate backend factories independently.
/// </summary>
/// <remarks>
/// <para>
/// Backend resolution:
/// <list type="bullet">
///   <item><description>When <c>EnableTelemetry</c> is <c>false</c>, all
///     operations route to <see cref="NullTelemetryFactory"/>.</description></item>
///   <item><description>Backend <c>OTLP</c> routes to
///     <see cref="OtlpTelemetryFactory"/>.</description></item>
///   <item><description>Any other backend name is looked up in the
///     registration table populated by
///     <see cref="RegisterTelemetryFactory"/> (for example, the
///     <c>AwsWrapperDataProvider.Telemetry.XRay</c> project registers
///     <c>"XRAY"</c>). Unregistered or unrecognized names fall back to
///     <see cref="NullTelemetryFactory"/>.</description></item>
/// </list>
/// </para>
/// <para>
/// The only trace-level adjustment performed here is converting
/// <see cref="TelemetryTraceLevel.TopLevel"/> to
/// <see cref="TelemetryTraceLevel.Nested"/> when
/// <c>TelemetrySubmitTopLevel</c> is <c>false</c>. All parent-detection
/// logic lives in the backend factories.
/// </para>
/// </remarks>
public sealed class DefaultTelemetryFactory : ITelemetryFactory
{
    private static readonly ConcurrentDictionary<string, ITelemetryFactory> RegisteredFactories = new();

    private readonly ITelemetryFactory tracesFactory;
    private readonly ITelemetryFactory metricsFactory;
    private readonly bool submitTopLevel;

    /// <summary>
    /// Initializes a new instance of the <see cref="DefaultTelemetryFactory"/>
    /// class by reading the telemetry-related connection properties.
    /// </summary>
    /// <param name="properties">The connection properties. Telemetry-related
    /// keys are optional; missing keys fall back to their property defaults.</param>
    public DefaultTelemetryFactory(Dictionary<string, string> properties)
    {
        bool enabled = PropertyDefinition.EnableTelemetry.GetBoolean(properties);
        this.submitTopLevel = PropertyDefinition.TelemetrySubmitTopLevel.GetBoolean(properties);

        if (!enabled)
        {
            this.tracesFactory = NullTelemetryFactory.Instance;
            this.metricsFactory = NullTelemetryFactory.Instance;
            return;
        }

        string tracesBackend = PropertyDefinition.TelemetryTracesBackend.GetString(properties)
            ?? string.Empty;
        string metricsBackend = PropertyDefinition.TelemetryMetricsBackend.GetString(properties)
            ?? string.Empty;

        this.tracesFactory = ResolveFactory(tracesBackend);
        this.metricsFactory = ResolveFactory(metricsBackend);
    }

    /// <summary>
    /// Registers a telemetry factory under the specified backend name so that
    /// it can be resolved when <c>TelemetryTracesBackend</c> or
    /// <c>TelemetryMetricsBackend</c> is set to that name. Backend names are
    /// compared case-insensitively.
    /// </summary>
    /// <remarks>
    /// The <c>OTLP</c> backend is built in and cannot be overridden. Passing
    /// <c>"OTLP"</c> as the name has no effect on routing.
    /// </remarks>
    /// <param name="name">The backend name (e.g., <c>"XRAY"</c>).</param>
    /// <param name="factory">The factory implementation to register.</param>
    public static void RegisterTelemetryFactory(string name, ITelemetryFactory factory)
    {
        RegisteredFactories[name.ToUpperInvariant()] = factory;
    }

    /// <summary>
    /// Removes a previously registered telemetry factory. Intended for test
    /// isolation.
    /// </summary>
    /// <param name="name">The backend name previously passed to
    /// <see cref="RegisterTelemetryFactory"/>.</param>
    internal static void UnregisterTelemetryFactory(string name)
    {
        RegisteredFactories.TryRemove(name.ToUpperInvariant(), out _);
    }

    /// <inheritdoc />
    public ITelemetryContext OpenTelemetryContext(string name, TelemetryTraceLevel traceLevel)
    {
        TelemetryTraceLevel effectiveLevel = traceLevel;
        if (!this.submitTopLevel && traceLevel == TelemetryTraceLevel.TopLevel)
        {
            effectiveLevel = TelemetryTraceLevel.Nested;
        }

        return this.tracesFactory.OpenTelemetryContext(name, effectiveLevel);
    }

    /// <inheritdoc />
    public void PostCopy(ITelemetryContext context, TelemetryTraceLevel traceLevel)
        => this.tracesFactory.PostCopy(context, traceLevel);

    /// <inheritdoc />
    public ITelemetryCounter CreateCounter(string name)
        => this.metricsFactory.CreateCounter(name);

    /// <inheritdoc />
    public ITelemetryGauge CreateGauge(string name, Func<long> valueCallback)
        => this.metricsFactory.CreateGauge(name, valueCallback);

    private static ITelemetryFactory ResolveFactory(string backend)
    {
        string normalized = backend.ToUpperInvariant();

        if (normalized == "OTLP")
        {
            return OtlpTelemetryFactory.Instance;
        }

        return RegisteredFactories.TryGetValue(normalized, out ITelemetryFactory? factory)
            ? factory
            : NullTelemetryFactory.Instance;
    }
}
