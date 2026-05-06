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
/// Specifies how a telemetry trace span should be created relative to any
/// surrounding trace context.
/// </summary>
public enum TelemetryTraceLevel
{
    /// <summary>
    /// Create the span as an independent top-level (root) span, even if a parent
    /// span is already active.
    /// </summary>
    ForceTopLevel,

    /// <summary>
    /// Create the span as a top-level (root) span with no parent.
    /// </summary>
    TopLevel,

    /// <summary>
    /// Create the span as a child of the current parent span.
    /// </summary>
    Nested,

    /// <summary>
    /// Suppress trace creation; the factory returns a no-op context.
    /// </summary>
    NoTrace,
}
