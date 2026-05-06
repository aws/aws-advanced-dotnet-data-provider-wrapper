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
/// Represents a gauge metric instrument that reports a point-in-time value.
/// </summary>
/// <remarks>
/// <para>
/// This is a marker interface. The gauge's current value is supplied via the
/// <see cref="Func{TResult}"/> callback passed to
/// <see cref="ITelemetryFactory.CreateGauge(string, Func{long})"/> at creation
/// time; the telemetry backend invokes the callback when it samples the gauge,
/// so the instrument itself exposes no mutation methods.
/// </para>
/// <para>
/// Implementations and their associated callbacks must be safe to observe
/// concurrently from multiple threads.
/// </para>
/// </remarks>
public interface ITelemetryGauge
{
}
