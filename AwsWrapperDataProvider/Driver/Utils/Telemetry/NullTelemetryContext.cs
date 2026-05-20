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
/// No-op implementation of <see cref="ITelemetryContext"/>. Returned by any
/// telemetry factory when tracing is disabled or when a requested trace level
/// is not applicable in the current context.
/// </summary>
/// <remarks>
/// This class is a stateless thread-safe singleton; all instance methods
/// deliberately perform no work.
/// </remarks>
public sealed class NullTelemetryContext : ITelemetryContext
{
    /// <summary>
    /// Fixed span name returned by <see cref="GetName"/> for identification
    /// in diagnostic output.
    /// </summary>
    private const string NullContextName = "NullTelemetryContext";

    /// <summary>
    /// Shared singleton instance.
    /// </summary>
    public static readonly NullTelemetryContext Instance = new();

    private NullTelemetryContext()
    {
    }

    /// <inheritdoc />
    public void SetSuccess(bool success)
    {
        // Intentionally empty.
    }

    /// <inheritdoc />
    public void SetAttribute(string key, string value)
    {
        // Intentionally empty.
    }

    /// <inheritdoc />
    public void SetException(Exception exception)
    {
        // Intentionally empty.
    }

    /// <inheritdoc />
    public string GetName() => NullContextName;

    /// <inheritdoc />
    public void CloseContext()
    {
        // Intentionally empty.
    }
}
