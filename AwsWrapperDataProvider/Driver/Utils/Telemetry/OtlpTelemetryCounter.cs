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

using System.Diagnostics.Metrics;

namespace AwsWrapperDataProvider.Driver.Utils.Telemetry;

/// <summary>
/// OTLP-backed <see cref="ITelemetryCounter"/> implementation that wraps a
/// <see cref="Counter{T}"/> from <c>System.Diagnostics.Metrics</c>.
/// </summary>
/// <remarks>
/// <see cref="Counter{T}"/> is thread-safe by design, so this wrapper is
/// inherently safe to use concurrently from multiple threads.
/// </remarks>
public sealed class OtlpTelemetryCounter : ITelemetryCounter
{
    private readonly Counter<long> counter;

    /// <summary>
    /// Initializes a new instance of the <see cref="OtlpTelemetryCounter"/>
    /// class wrapping the specified <see cref="Counter{T}"/>.
    /// </summary>
    /// <param name="counter">The underlying counter instrument.</param>
    public OtlpTelemetryCounter(Counter<long> counter)
    {
        this.counter = counter;
    }

    /// <inheritdoc />
    public void Add(long value) => this.counter.Add(value);

    /// <inheritdoc />
    public void Inc() => this.counter.Add(1);
}
