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
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using Microsoft.Extensions.Caching.Memory;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class EfmConnectivityTests
{
    private static readonly int TestCommandTimeoutSecs = 500;

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public async Task EfmPluginTest_WithBasicAuth()
    {
        const string clusterEndpoint = "endpoint"; // Replace with your cluster endpoint
        const string username = "username"; // Replace with your username
        const string password = "password"; // Replace with your password
        const string database = "database"; // Replace with your database name

        var connectionString = $"Host={clusterEndpoint};Username={username};Password={password};Database={database};Port=5432;Plugins=efm;";
        await PerformEfmTest(connectionString);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    public async Task EfmPluginTest_WithIamAuth()
    {
        const string clusterEndpoint = "endpoint"; // Replace with your cluster endpoint
        const string dbUser = "username"; // Replace with the name of the db user with IAM auth
        const string database = "database"; // Replace with your database name

        var connectionString = $"Host={clusterEndpoint};Username={dbUser};Database={database};Plugins=iam,efm;";
        await PerformEfmTest(connectionString);
    }

    private static async Task PerformEfmTest(string connectionString)
    {
        try
        {
            AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);

            Console.WriteLine("1. Opening initial connection...");
            connection.Open();
            Console.WriteLine("   ✓ Connected successfully");
            Console.WriteLine($"   Connection State: {connection.State}");

            Console.WriteLine("\n2. Identifying current host...");
            string host = GetHost(connection);
            Console.WriteLine($"   Current host: {host}");
            string monitorKey = HostMonitorService.GetMonitorKey(
                HostMonitoringPlugin.DefaultFailureDetectionTime,
                HostMonitoringPlugin.DefaultFailureDetectionInterval,
                HostMonitoringPlugin.DefaultFailureDetectionCount,
                host);
            Console.WriteLine($"   Monitor key: {monitorKey}");

            AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
            command.CommandText = $"SELECT pg_sleep({TestCommandTimeoutSecs}), now() as query_time, inet_server_addr()::text as server_ip";
            command.CommandTimeout = TestCommandTimeoutSecs; // command won't time out before the monitoring catches a disconnection

            Console.WriteLine("\n3. Running long command in background task...");
            Task<DbDataReader> readerTask = command.ExecuteReaderAsync();

            await Task.Delay(HostMonitoringPlugin.DefaultFailureDetectionTime + 100, TestContext.Current.CancellationToken);

            Console.WriteLine("\n4. Asserting that monitor exists...");
            Assert.True(HostMonitorService.Monitors.TryGetValue(monitorKey, out IHostMonitor? monitor) && monitor != null);
            Assert.False(monitor.CanDispose());
            Console.WriteLine("   ✓ Monitor exists");

            bool monitorCaughtFailure = false;

            Console.WriteLine("\n5. Monitoring for failure...");
            Console.WriteLine("   ! Please turn off your internet or cause the connection to become unresponsive during this time.");
            for (int timeWaited = 0; timeWaited < TestCommandTimeoutSecs * 1000; timeWaited += HostMonitoringPlugin.DefaultFailureDetectionInterval)
            {
                await Task.Delay(HostMonitoringPlugin.DefaultFailureDetectionInterval);
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
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   ❌ Test failed due to uncaught exception: {ex.Message}");
        }
    }

    private static string GetHost(AwsWrapperConnection<NpgsqlConnection> connection)
    {
        using var command = connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = "SELECT inet_server_addr()::text as server_ip;";

        string? host = null;
        using var reader = command.ExecuteReader();
        if (reader.Read())
        {
            host = reader.IsDBNull("server_ip") ? null : reader.GetString("server_name");
        }

        return host ?? throw new Exception("Could not get host.");
    }
}
