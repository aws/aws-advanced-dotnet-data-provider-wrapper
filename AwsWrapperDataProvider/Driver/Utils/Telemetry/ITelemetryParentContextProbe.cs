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
/// Optional companion to <see cref="ITelemetryFactory"/>: lets a backend
/// expose whether its native trace context currently has a parent span.
/// </summary>
/// <remarks>
/// <para>
/// Implementations must be safe to call concurrently from multiple threads
/// and must not throw.
/// </para>
/// </remarks>
public interface ITelemetryParentContextProbe
{
    /// <summary>
    /// Returns a snapshot of whether a parent trace context exists on the
    /// calling logical thread.
    /// </summary>
    /// <returns><see langword="true"/> when a parent span is active in the
    /// backend's native trace context; otherwise <see langword="false"/>.</returns>
    bool HasParentContext();
}
