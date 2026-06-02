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
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Utils.Telemetry;

/// <summary>
/// Routing <see cref="ITelemetryFactory"/> that reads the telemetry-related
/// connection properties and delegates trace and metric operations to the appropriate backend factories.
/// </summary>
public sealed class DefaultTelemetryFactory : ITelemetryFactory
{
    private static readonly ILogger<DefaultTelemetryFactory> Logger =
        LoggerUtils.GetLogger<DefaultTelemetryFactory>();

    private static readonly ConcurrentDictionary<string, ITelemetryFactory> RegisteredFactories = new();

    private readonly ITelemetryFactory tracesFactory;
    private readonly ITelemetryParentContextProbe? tracesProbe;
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
            this.tracesProbe = this.tracesFactory as ITelemetryParentContextProbe;
            return;
        }

        string tracesBackend = PropertyDefinition.TelemetryTracesBackend.GetString(properties)
            ?? string.Empty;
        string metricsBackend = PropertyDefinition.TelemetryMetricsBackend.GetString(properties)
            ?? string.Empty;

        this.tracesFactory = ResolveFactory(tracesBackend);
        this.metricsFactory = ResolveFactory(metricsBackend);

        // Resolve the parent-context probe once per factory instance so that
        // OpenTelemetryContext does not pay a runtime type check per call.
        this.tracesProbe = this.tracesFactory as ITelemetryParentContextProbe;
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
    /// <exception cref="ArgumentNullException"><paramref name="name"/> or
    /// <paramref name="factory"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> is empty
    /// or contains only whitespace.</exception>
    public static void RegisterTelemetryFactory(string name, ITelemetryFactory factory)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(factory);
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException(
                Resources.DefaultTelemetryFactory_RegisterTelemetryFactory_EmptyName,
                nameof(name));
        }

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
        // Probe parent state and resolve the effective level here so backends
        // can be straight appliers. The probe is queried at most once per
        // OpenTelemetryContext call, immediately before delegation; any window
        // between probe and StartActivity is closed by the AsyncLocal scoping
        // of the underlying parent state.
        bool parentExists = this.tracesProbe?.HasParentContext() ?? false;
        TelemetryTraceLevel effectiveLevel = ResolveLevel(traceLevel, parentExists, this.submitTopLevel);
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

    /// <summary>
    /// Computes the effective <see cref="TelemetryTraceLevel"/> for the
    /// requested level given the current parent state and the
    /// <c>TelemetrySubmitTopLevel</c> setting.
    /// </summary>
    /// <param name="requested">The level the caller asked for.</param>
    /// <param name="parentExists">Whether the trace backend reports an
    /// active parent span.</param>
    /// <param name="submitTopLevel">The value of the
    /// <c>TelemetrySubmitTopLevel</c> property.</param>
    /// <returns>The level that should be passed to the backend.</returns>
    internal static TelemetryTraceLevel ResolveLevel(
        TelemetryTraceLevel requested,
        bool parentExists,
        bool submitTopLevel)
    {
        // ForceTopLevel and NoTrace bypass the matrix entirely.
        if (requested == TelemetryTraceLevel.ForceTopLevel)
        {
            return TelemetryTraceLevel.ForceTopLevel;
        }

        if (requested == TelemetryTraceLevel.NoTrace)
        {
            return TelemetryTraceLevel.NoTrace;
        }

        if (!submitTopLevel)
        {
            // submitTopLevel = false: nest under any parent we find;
            // promote a TopLevel request to root only when no parent exists.
            if (parentExists)
            {
                return TelemetryTraceLevel.Nested;
            }

            return requested == TelemetryTraceLevel.TopLevel
                ? TelemetryTraceLevel.TopLevel
                : TelemetryTraceLevel.Nested;
        }

        // submitTopLevel = true (override): TopLevel always wins; a Nested
        // request still requires a parent, or it is dropped.
        if (requested == TelemetryTraceLevel.TopLevel)
        {
            return TelemetryTraceLevel.TopLevel;
        }

        return parentExists
            ? TelemetryTraceLevel.Nested
            : TelemetryTraceLevel.NoTrace;
    }

    private static ITelemetryFactory ResolveFactory(string backend)
    {
        string normalized = backend.ToUpperInvariant();

        if (normalized == "OTLP")
        {
            return OtlpTelemetryFactory.Instance;
        }

        if (RegisteredFactories.TryGetValue(normalized, out ITelemetryFactory? factory))
        {
            return factory;
        }

        // Only log when the caller provided a non-empty value that we did not
        // recognize. An empty string or "NONE" is the documented disabled value
        // and should not produce log noise.
        if (normalized.Length > 0 && normalized != "NONE")
        {
            Logger.LogDebug(
                Resources.DefaultTelemetryFactory_ResolveFactory_UnknownBackend,
                backend);
        }

        return NullTelemetryFactory.Instance;
    }
}
