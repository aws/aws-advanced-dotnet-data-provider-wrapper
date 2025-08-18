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
    private static readonly int TestFailureDetectionTime = 1000;
    private static readonly int TestFailureDetectionInterval = 5000;
    private static readonly int TestFailureDetectionCount = 1;

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public async Task EfmPluginTest_WithIamAuth()
    {
        const string clusterEndpoint = "endpoint"; // Replace with your cluster endpoint
        const string dbUser = "username"; // Replace with the name of the db user with IAM auth
        const string database = "database"; // Replace with your database name

        var connectionString = $"Host={clusterEndpoint};Username={dbUser};Database={database};Plugins=iam,efm;";
        await PerformEfmTest(connectionString, clusterEndpoint);
    }

    internal static async Task PerformEfmTest(string connectionString, string initialHost)
    {
        connectionString += $"FailureDetectionTime={TestFailureDetectionTime};FailureDetectionInterval={TestFailureDetectionInterval};FailureDetectionCount={TestFailureDetectionCount};";

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
                TestFailureDetectionTime,
                TestFailureDetectionInterval,
                TestFailureDetectionCount,
                host);
            Console.WriteLine($"   Monitor key: {monitorKey}");

            AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
            command.CommandText = $"SELECT pg_sleep({TestCommandTimeoutSecs}), now() as query_time, inet_server_addr()::text as server_ip";
            command.CommandTimeout = TestCommandTimeoutSecs; // command won't time out before the monitoring catches a disconnection

            Console.WriteLine("\n3. Running long command in background task...");
            var readerTask = command.ExecuteReaderAsync();

            await Task.Delay(TestFailureDetectionTime + 1000, TestContext.Current.CancellationToken);

            Console.WriteLine("\n4. Asserting that monitor exists...");
            Assert.True(HostMonitorService.Monitors.TryGetValue(monitorKey, out IHostMonitor? monitor) && monitor != null);
            Assert.False(monitor.CanDispose());
            Console.WriteLine("   ✓ Monitor exists");

            bool monitorCaughtFailure = false, disabledConnectivity = false;

            Console.WriteLine("\n5. Monitoring for failure...");
            Console.WriteLine("   ! Please turn off your internet or cause the connection to become unresponsive during this time.");
            for (int timeWaited = 0; timeWaited < TestCommandTimeoutSecs * 1000; timeWaited += TestFailureDetectionInterval)
            {
                await Task.Delay(TestFailureDetectionInterval);
                if (monitor.CanDispose())
                {
                    Console.WriteLine("   ✓ Monitor has caught the failure.");
                    monitorCaughtFailure = true;

                    if (connection.State != ConnectionState.Open)
                    {
                        Console.WriteLine("   ✓ Monitor has disposed of the connection.");
                    }

                    break;
                }
                else if (timeWaited > 0 && !disabledConnectivity)
                {
                    ProxyHelper.DisableAllConnectivity();
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

            if (connection.State == ConnectionState.Open)
            {
                Console.WriteLine("   ❌ Monitor did not dispose the connection.");
                connection.Close();
                connection.Dispose();
            }

            Assert.NotEqual(ConnectionState.Open, connection.State);
            Assert.True(monitorCaughtFailure);

            // done with the test; restore proxy connectivity
            ProxyHelper.EnableAllConnectivity();
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
