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

using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Tests.Container.Utils;

namespace AwsWrapperDataProvider.Tests;

public class EfmConnectivityIntegrationTests : IntegrationTestBase
{
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public async Task EfmPluginTest_WithDefaultConfiguration()
    {
        int failureDetectionTime = HostMonitoringPlugin.DefaultFailureDetectionTime;
        int failureDetectionInterval = HostMonitoringPlugin.DefaultFailureDetectionInterval;
        int failureDetectionCount = HostMonitoringPlugin.DefaultFailureDetectionCount;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, Username, Password, DefaultDbName, plugins: "efm");
        await EfmConnectivityTests.PerformEfmTest(connectionString, ClusterEndpoint, failureDetectionTime, failureDetectionInterval, failureDetectionCount);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public async Task EfmPluginTest_WithFailureFailureCount1()
    {
        int failureDetectionTime = 5000; // start monitoring after 5 seconds
        int failureDetectionInterval = HostMonitoringPlugin.DefaultFailureDetectionInterval;
        int failureDetectionCount = 1;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, Username, Password, DefaultDbName, plugins: "efm");
        connectionString += $";FailureDetectionTime={failureDetectionTime};FailureDetectionCount={failureDetectionCount};";
        await EfmConnectivityTests.PerformEfmTest(connectionString, ClusterEndpoint, failureDetectionTime, failureDetectionInterval, failureDetectionCount);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public async Task EfmPluginTest_WithSpecialConfiguration()
    {
        int failureDetectionTime = 1000; // start monitoring after one second
        int failureDetectionInterval = 500; // check on the connection every 500 ms
        int failureDetectionCount = 5; // five failures before considered unhealthy

        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, Username, Password, DefaultDbName, plugins: "efm");
        connectionString += $";FailureDetectionTime={failureDetectionTime};FailureDetectionInterval={failureDetectionInterval};FailureDetectionCount={failureDetectionCount};";
        await EfmConnectivityTests.PerformEfmTest(connectionString, ClusterEndpoint, failureDetectionTime, failureDetectionInterval, failureDetectionCount);
    }
}
