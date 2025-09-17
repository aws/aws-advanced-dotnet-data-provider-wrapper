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

using System.Data;
using System.Data.Common;
using System.Diagnostics;
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Tests.Container.Utils;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class EfmConnectivityIntegrationTests : IntegrationTestBase
{
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EfmPluginTest_WithDefaultConfiguration()
    {
        int failureDetectionTime = HostMonitoringPlugin.DefaultFailureDetectionTime;
        int failureDetectionInterval = HostMonitoringPlugin.DefaultFailureDetectionInterval;
        int failureDetectionCount = HostMonitoringPlugin.DefaultFailureDetectionCount;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, plugins: "efm");
        await EfmConnectivityTests.PerformEfmTest(connectionString, Endpoint, failureDetectionTime, failureDetectionInterval, failureDetectionCount);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EfmPluginTest_WithFailureFailureCount1()
    {
        int failureDetectionTime = 5000; // start monitoring after 5 seconds
        int failureDetectionInterval = HostMonitoringPlugin.DefaultFailureDetectionInterval;
        int failureDetectionCount = 1;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, plugins: "efm");
        connectionString += $";FailureDetectionTime={failureDetectionTime};FailureDetectionCount={failureDetectionCount};";
        await EfmConnectivityTests.PerformEfmTest(connectionString, Endpoint, failureDetectionTime, failureDetectionInterval, failureDetectionCount);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EfmPluginTest_WithSpecialConfiguration()
    {
        int failureDetectionTime = 1000; // start monitoring after one second
        int failureDetectionInterval = 500; // check on the connection every 500 ms
        int failureDetectionCount = 5; // five failures before considered unhealthy

        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, plugins: "efm");
        connectionString += $";FailureDetectionTime={failureDetectionTime};FailureDetectionInterval={failureDetectionInterval};FailureDetectionCount={failureDetectionCount};";
        await EfmConnectivityTests.PerformEfmTest(connectionString, Endpoint, failureDetectionTime, failureDetectionInterval, failureDetectionCount);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Database", "mysql")]
    public async Task EfmPluginTest_NetworkFailureDetection()
    {
        int failureDelaySec = 10;
        int maxDurationsSec = 30;
        var instance1 = ProxyDatabaseInfo.Instances[0].Host;
        var port = ProxyDatabaseInfo.Instances[0].Port;
        var instanceId = ProxyDatabaseInfo.Instances[0].InstanceId;

        var connectionString = ConnectionStringHelper.GetUrl(Engine, instance1, port, Username, Password, DefaultDbName, commandTimeout: maxDurationsSec, connectionTimeout: 10, plugins: "efm");
        connectionString += $";FailureDetectionTime={5000};FailureDetectionCount=1;";

        using AwsWrapperConnection connection = Engine switch
        {
            DatabaseEngine.MYSQL => new AwsWrapperConnection<MySqlConnection>(connectionString),
            DatabaseEngine.PG => new AwsWrapperConnection<NpgsqlConnection>(connectionString),
            _ => throw new NotSupportedException($"Unsupported engine: {Engine}"),
        };
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);

        using var command = connection.CreateCommand();
        command.CommandText = AuroraUtils.GetSleepSql(Engine, maxDurationsSec);

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var simulationTask = AuroraUtils.SimulateTemporaryFailureTask(instance1, TimeSpan.FromSeconds(failureDelaySec), TimeSpan.FromSeconds(maxDurationsSec), tcs);

        var stopwatch = Stopwatch.StartNew();
        try
        {
            command.ExecuteScalar();
            Assert.Fail("Sleep query should have failed");
        }
        catch (DbException)
        {
            var durationMs = stopwatch.Elapsed.TotalMilliseconds;
            Assert.True(
                durationMs > failureDelaySec * 1000 && durationMs < maxDurationsSec * 1000,
                $"Time before failure was not between {failureDelaySec} and {maxDurationsSec} seconds, actual duration was {durationMs / 1000} seconds.");
        }

        await tcs.Task;
        await simulationTask;
    }
}
