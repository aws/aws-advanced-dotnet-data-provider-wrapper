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
/// No-op implementation of <see cref="ITelemetryFactory"/>. Returns the
/// shared singleton Null instances for every instrument so that plugins can
/// call telemetry APIs unconditionally when telemetry is disabled.
/// </summary>
/// <remarks>
/// This class is a stateless thread-safe singleton.
/// </remarks>
public sealed class NullTelemetryFactory : ITelemetryFactory
{
    /// <summary>
    /// Shared singleton instance.
    /// </summary>
    public static readonly NullTelemetryFactory Instance = new();

    private NullTelemetryFactory()
    {
    }

    /// <inheritdoc />
    public ITelemetryContext OpenTelemetryContext(string name, TelemetryTraceLevel traceLevel)
        => NullTelemetryContext.Instance;

    /// <inheritdoc />
    public void PostCopy(ITelemetryContext context, TelemetryTraceLevel traceLevel)
    {
        // Intentionally empty.
    }

    /// <inheritdoc />
    public ITelemetryCounter CreateCounter(string name) => NullTelemetryCounter.Instance;

    /// <inheritdoc />
    public ITelemetryGauge CreateGauge(string name, Func<long> valueCallback) => NullTelemetryGauge.Instance;
}
