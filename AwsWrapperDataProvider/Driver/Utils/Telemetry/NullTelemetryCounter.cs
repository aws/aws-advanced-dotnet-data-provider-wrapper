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
/// No-op implementation of <see cref="ITelemetryCounter"/>. Returned by any
/// telemetry factory when metrics are disabled.
/// </summary>
/// <remarks>
/// This class is a stateless thread-safe singleton; all instance methods
/// deliberately perform no work.
/// </remarks>
public sealed class NullTelemetryCounter : ITelemetryCounter
{
    /// <summary>
    /// Shared singleton instance.
    /// </summary>
    public static readonly NullTelemetryCounter Instance = new();

    private NullTelemetryCounter()
    {
    }

    /// <inheritdoc />
    public void Add(long value)
    {
        // Intentionally empty.
    }

    /// <inheritdoc />
    public void Inc()
    {
        // Intentionally empty.
    }
}
