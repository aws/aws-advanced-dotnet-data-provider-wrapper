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
    private static readonly int TestCommandTimeoutSecs = 500;

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
        await PerformEfmTest(connectionString, clusterEndpoint, true, failureDetectionTime, failureDetectionInterval, failureDetectionCount);
    }

    internal static async Task PerformEfmTest(string connectionString, string initialHost, bool isManualTest, int failureDetectionTime, int failureDetectionInterval, int failureDetectionCount)
    {
        try
        {
            AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);

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

            AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
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

            if (isManualTest)
            {
                Console.WriteLine("   ! Please turn off your internet or cause the connection to become unresponsive during this time.");
            }

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
                else if (timeWaited > 0 && !disabledConnectivity && !isManualTest)
                {
                    await ProxyHelper.DisableAllConnectivityAsync();
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

            if (monitorFailureCount != failureDetectionCount)
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
            Assert.Equal(monitorFailureCount, failureDetectionCount);
            Assert.NotEqual(ConnectionState.Open, connection.State);

            if (!isManualTest)
            {
                // done with the test; restore proxy connectivity
                await ProxyHelper.EnableAllConnectivityAsync();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   ❌ Test failed due to unexpected exception: {ex.Message}");
            throw new Exception("EFM2 plugin integration test failed.", ex);
        }
    }

    private static string GetConnectedHost(AwsWrapperConnection<NpgsqlConnection> connection, string initialHost)
    {
        using var command = connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = "SELECT aurora_db_instance_identifier()::text as server_name;";

        using var reader = command.ExecuteReader();
        if (reader.Read() && !reader.IsDBNull("server_name"))
        {
            var hostName = reader.GetString("server_name");
            string clusterInstanceTemplate = RdsUtils.GetRdsInstanceHostPattern(initialHost);
            return clusterInstanceTemplate.Replace("?", hostName);
        }

        return initialHost;
    }
}
