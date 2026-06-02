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

namespace AwsWrapperDataProvider.Driver.Utils.Telemetry;

/// <summary>
/// Factory abstraction for creating telemetry trace contexts and metric
/// instruments.
/// </summary>
/// <remarks>
/// <para>
/// Implementations must be safe to use concurrently from multiple threads.
/// </para>
/// </remarks>
public interface ITelemetryFactory
{
    /// <summary>
    /// Opens a new telemetry trace context (span) with the specified name and
    /// trace level.
    /// </summary>
    /// <param name="name">The span name. </param>
    /// <param name="traceLevel">Controls whether the span is created as
    /// top-level, nested, or suppressed.</param>
    /// <returns>
    /// A new <see cref="ITelemetryContext"/>. Maybe a no-op context when
    /// telemetry is disabled or when the requested.
    /// </returns>
    ITelemetryContext OpenTelemetryContext(string name, TelemetryTraceLevel traceLevel);

    /// <summary>
    /// Submits a clone of an already-closed trace context as an independent
    /// trace at the specified level.
    /// </summary>
    /// <remarks>
    /// The source context must have been closed via
    /// <see cref="ITelemetryContext.CloseContext"/> before calling this
    /// method so that start and end timestamps are available on the clone.
    /// </remarks>
    /// <param name="context">The completed context to copy.</param>
    /// <param name="traceLevel">The trace level at which to submit the
    /// clone; typically <see cref="TelemetryTraceLevel.ForceTopLevel"/>.</param>
    void PostCopy(ITelemetryContext context, TelemetryTraceLevel traceLevel);

    /// <summary>
    /// Creates a new monotonically increasing counter instrument with the
    /// specified name.
    /// </summary>
    /// <param name="name">The counter name.</param>
    /// <returns>A new <see cref="ITelemetryCounter"/>, or a no-op counter
    /// when metrics are disabled.</returns>
    ITelemetryCounter CreateCounter(string name);

    /// <summary>
    /// Creates a new gauge instrument with the specified name.
    /// </summary>
    /// <param name="name">The gauge name.</param>
    /// <param name="valueCallback">Callback invoked by the telemetry backend
    /// to read the gauge's current value.</param>
    /// <returns>A new <see cref="ITelemetryGauge"/>, or a no-op gauge when
    /// metrics are disabled.</returns>
    ITelemetryGauge CreateGauge(string name, Func<long> valueCallback);
}
