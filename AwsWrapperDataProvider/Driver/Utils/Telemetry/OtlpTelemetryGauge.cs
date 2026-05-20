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
/// OTLP-backed <see cref="ITelemetryGauge"/> marker returned by
/// <see cref="OtlpTelemetryFactory.CreateGauge"/>. The
/// <c>ObservableGauge&lt;long&gt;</c> and its observe callback are registered
/// with the shared <c>Meter</c> at creation time; the caller does not
/// interact with this instance after it is returned.
/// </summary>
/// <remarks>
/// The instrument is kept alive by the <c>Meter</c>'s internal instrument
/// list, so this marker does not need to hold a reference.
/// </remarks>
public sealed class OtlpTelemetryGauge : ITelemetryGauge
{
}
