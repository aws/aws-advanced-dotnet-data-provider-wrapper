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
/// Represents a monotonically increasing counter metric instrument.
/// </summary>
/// <remarks>
/// Implementations must be safe to use concurrently from multiple threads.
/// </remarks>
public interface ITelemetryCounter
{
    /// <summary>
    /// Increments the counter by the specified value.
    /// </summary>
    /// <param name="value">The non-negative amount to add to the counter.</param>
    void Add(long value);

    /// <summary>
    /// Increments the counter by one. Equivalent to <c>Add(1)</c>.
    /// </summary>
    void Inc();
}
