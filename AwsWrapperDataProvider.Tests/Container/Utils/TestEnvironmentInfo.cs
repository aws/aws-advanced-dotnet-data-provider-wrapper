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

namespace AwsWrapperDataProvider.Tests.Container.Utils;

public class TestEnvironmentInfo
{
    public TestEnvironmentRequest Request { get; set; } = null!;
    public string? AwsAccessKeyId { get; set; }
    public string? AwsSecretAccessKey { get; set; }
    public string? AwsSessionToken { get; set; }

    public string? Region { get; set; }
    public string? RdsEndpoint { get; set; }
    public string? RdsDbName { get; set; }
    public string? IamUsername { get; set; }

    public TestDatabaseInfo DatabaseInfo { get; set; } = null!;
    public TestProxyDatabaseInfo? ProxyDatabaseInfo { get; set; }
    public string DatabaseEngine { get; set; } = null!;
    public string DatabaseEngineVersion { get; set; } = null!;
    public TestTelemetryInfo? TracesTelemetryInfo { get; set; }
    public TestTelemetryInfo? MetricsTelemetryInfo { get; set; }

    public string? BlueGreenDeploymentId { get; set; }

    public string? ClusterParameterGroupName { get; set; }

    // Random alphanumeric combination that is used to form a test cluster name or an instance name.
    public string? RandomBase { get; set; }
}
