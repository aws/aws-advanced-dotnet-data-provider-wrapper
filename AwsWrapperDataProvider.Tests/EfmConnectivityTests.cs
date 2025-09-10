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
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.Extensions.Caching.Memory;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class EfmConnectivityTests
{
    private static readonly AuroraTestUtils AuroraUtils = AuroraTestUtils.GetUtility();
    private static readonly int TestCommandTimeoutSecs = 500;

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public async Task EfmPluginTest_WithBasicAuth()
    {
        const string clusterEndpoint = "endpoint"; // Replace with your cluster endpoint
        const string user = "username"; // Replace with the name of the db user to connect with
        const string password = "password"; // Replace with password of the db user to connect with
        const string database = "database"; // Replace with your database name

        int failureDetectionTime = 1000; // start monitoring after one second
        int failureDetectionInterval = 1000; // check on the connection every 1 second
        int failureDetectionCount = 1; // five failures before considered unhealthy

        var connectionString = $"Host={clusterEndpoint};Username={user};Password={password};Database={database};";
        connectionString += $"; Plugins=efm;FailureDetectionTime={failureDetectionTime};FailureDetectionInterval={failureDetectionInterval};FailureDetectionCount={failureDetectionCount};";
        await PerformEfmTest(connectionString, clusterEndpoint, failureDetectionTime, failureDetectionInterval, failureDetectionCount);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public async Task EfmPluginTest_WithIamAuth()
    {
        const string clusterEndpoint = "endpoint"; // Replace with your cluster endpoint
        const string dbUser = "username"; // Replace with the name of the db user with IAM auth
        const string database = "database"; // Replace with your database name

        const int failureDetectionTime = 1000;
        const int failureDetectionInterval = 5000;
        const int failureDetectionCount = 1;

        var connectionString = $"Host={clusterEndpoint};Username={dbUser};Database={database};Plugins=iam,efm;"
            + $"FailureDetectionTime={failureDetectionTime};FailureDetectionInterval={failureDetectionInterval};FailureDetectionCount={failureDetectionCount};";
        await PerformEfmTest(connectionString, clusterEndpoint, failureDetectionTime, failureDetectionInterval, failureDetectionCount);
    }

    internal static async Task PerformEfmTest(string connectionString, string initialHost, int failureDetectionTime, int failureDetectionInterval, int failureDetectionCount)
    {
        try
        {
            using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);

            Console.WriteLine("1. Opening initial connection...");
            connection.Open();
            Console.WriteLine("   ✓ Connected successfully");
            Console.WriteLine($"   Connection State: {connection.State}");

            Console.WriteLine("2. Identifying host...");
            string host = GetConnectedHost(connection, initialHost);
            Console.WriteLine($"   Host: {host}");
            string monitorKey = HostMonitorService.GetMonitorKey(
                failureDetectionTime,
                failureDetectionInterval,
                failureDetectionCount,
                host);
            Console.WriteLine($"   Monitor key: {monitorKey}");

            using AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
            command.CommandText = $"SELECT pg_sleep({TestCommandTimeoutSecs}), now() as query_time, inet_server_addr()::text as server_ip";
            command.CommandTimeout = TestCommandTimeoutSecs; // command won't time out before the monitoring catches a disconnection

            Console.WriteLine("\n3. Running long command in background task...");
            var readerTask = command.ExecuteReaderAsync();

            await Task.Delay(failureDetectionTime + 1000, TestContext.Current.CancellationToken);

            Console.WriteLine("\n4. Asserting that monitor exists...");
            Assert.True(HostMonitorService.Monitors.TryGetValue(monitorKey, out IHostMonitor? monitor) && monitor != null);
            Assert.False(monitor.CanDispose());
            Console.WriteLine("   ✓ Monitor exists");

            bool disabledConnectivity = false, monitorCaughtFailure = false;
            int monitorFailureCount = 0;

            Console.WriteLine("\n5. Monitoring for failure...");

            for (int timeWaited = 0; timeWaited < TestCommandTimeoutSecs * 1000; timeWaited += failureDetectionInterval)
            {
                await Task.Delay(failureDetectionInterval);
                if (monitor.CanDispose())
                {
                    Console.WriteLine("   ✓ Monitor has caught the failure.");
                    monitorCaughtFailure = true;
                    monitorFailureCount = monitor.FailureCount;

                    if (connection.State != ConnectionState.Open)
                    {
                        Console.WriteLine("   ✓ Monitor has disposed of the connection.");
                    }

                    break;
                }
                else if (timeWaited > failureDetectionInterval * 2 && !disabledConnectivity)
                {
                    // The testUnhealthyCluster boolean simulates the following scenario:
                    // - original connection is not being dropped (connection is kept alive), but server is unresponsive
                    // - new commands are unresponsive / fail
                    // which results in the failure counter correctly accumulating; disabling connectivity with toxiproxy
                    // or turning off internet on a manual testing device results in the original connection failing early,
                    // and thus the monitor is not an effective catcher of the failure.
                    // In essence, the monitor is most effective when something goes wrong on the cluster, not necessarily the network.
                    ((HostMonitor)monitor).TestUnhealthyCluster = true;
                    disabledConnectivity = true;
                }
            }

            try
            {
                await readerTask;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"   ✓ Reader has thrown an exception: {ex.Message}");
            }

            if (!monitorCaughtFailure)
            {
                Console.WriteLine("   ❌ Monitor did not catch the failure.");
            }

            if (monitorFailureCount < failureDetectionCount)
            {
                Console.WriteLine($"   ❌ Monitor caught {monitorFailureCount} failures when configured to handle {failureDetectionCount} failures.");
            }

            if (connection.State == ConnectionState.Open)
            {
                Console.WriteLine("   ❌ Monitor did not dispose the connection.");
                connection.Close();
                connection.Dispose();
            }

            Assert.True(monitorCaughtFailure);
            Assert.True(monitorFailureCount >= failureDetectionCount);
            Assert.NotEqual(ConnectionState.Open, connection.State);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   ❌ Test failed due to unexpected exception: {ex.Message}");
            throw new Exception("EFM2 plugin integration test failed.", ex);
        }
    }

    private static string GetConnectedHost(IDbConnection connection, string initialHost)
    {
        string hostName = AuroraUtils.QueryInstanceId(connection);
        string clusterInstanceTemplate = RdsUtils.GetRdsInstanceHostPattern(initialHost);
        return clusterInstanceTemplate.Replace("?", hostName);
    }
}
