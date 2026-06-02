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

using AwsWrapperDataProvider.Driver.Utils.Telemetry;

namespace AwsWrapperDataProvider.Telemetry.XRay;

/// <summary>
/// Entry point used by application code to register the X-Ray telemetry
/// backend with the core <see cref="DefaultTelemetryFactory"/>.
/// </summary>
public static class XRayTelemetryLoader
{
    public const string BackendName = "XRAY";

    public static void Load()
    {
        DefaultTelemetryFactory.RegisterTelemetryFactory(BackendName, new XRayTelemetryFactory());
    }
}
