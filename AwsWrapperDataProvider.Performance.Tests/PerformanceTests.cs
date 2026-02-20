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

using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics;
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using Npgsql;
using Xunit;

namespace AwsWrapperDataProvider.Performance.Tests;

public class PerformanceTests
{
    private static readonly ILogger<HostMonitoringPlugin> Logger = LoggerUtils.GetLogger<HostMonitoringPlugin>();

    private static readonly int RepeatTimes =
        string.IsNullOrEmpty(Environment.GetEnvironmentVariable("REPEAT_TIMES"))
            ? 5
            : int.Parse(Environment.GetEnvironmentVariable("REPEAT_TIMES") ?? "5");

    private static readonly ConcurrentQueue<PerformanceStatistics.PerformanceStatistics> EfmPerformanceDataList = new();
    private static readonly ConcurrentQueue<PerformanceStatistics.PerformanceStatistics> FailoverAndEfmPerformanceDataList = new();

    private static readonly string DefaultDbName = TestEnvironment.Env.Info.DatabaseInfo.DefaultDbName;
    private static readonly string Username = TestEnvironment.Env.Info.DatabaseInfo.Username;
    private static readonly string Password = TestEnvironment.Env.Info.DatabaseInfo.Password;
    private static readonly DatabaseEngine Engine = TestEnvironment.Env.Info.Request.Engine;
    private static readonly string CurrentDatabaseEngine = TestEnvironment.Env.Info.DatabaseEngine;

    private static string GetQuerySql(int seconds)
    {
        return Engine switch
        {
            DatabaseEngine.PG => $"SELECT pg_sleep({seconds})",
            DatabaseEngine.MYSQL or DatabaseEngine.MARIADB => $"SELECT SLEEP({seconds})",
            _ => throw new NotSupportedException($"Unsupported engine: {Engine}"),
        };
    }

    private static DbConnection OpenConnectionWithRetry(string connectionString)
    {
        AwsWrapperConnection? connection = null;
        int connectCount = 0;

        while (connection == null && connectCount < 10)
        {
            try
            {
                connection = Engine switch
                {
                    DatabaseEngine.MYSQL => new AwsWrapperConnection<MySqlConnection>(connectionString),
                    DatabaseEngine.PG => new AwsWrapperConnection<NpgsqlConnection>(connectionString),
                    _ => throw new NotSupportedException($"Unsupported engine: {Engine}"),
                };

                connection.Open();
                return connection;
            }
            catch (Exception ex)
            {
                Logger.LogTrace($@"Connection attempt {connectCount + 1} failed: {ex.GetType().Name}: {ex.Message}");

                connection?.Dispose();
                connection = null;
                connectCount++;

                if (connectCount < 10)
                {
                    Thread.Sleep(1000);
                }
            }
        }

        throw new Exception($@"Failed to establish connection after {connectCount} attempts");
    }

    private static IEnumerable<int[]> GenerateFailureDetectionTimeParameters()
    {
        return
        [
            [30000, 5000, 3, 10000],
            [30000, 5000, 3, 15000],
            [30000, 5000, 3, 20000],
            [30000, 5000, 3, 25000],
            [30000, 5000, 3, 30000],
            [30000, 5000, 3, 35000],
            [30000, 5000, 3, 40000],
            [30000, 5000, 3, 45000],
            [30000, 5000, 3, 50000],
            [30000, 5000, 3, 55000],
            [30000, 5000, 3, 60000],

            [6000, 1000, 1, 1000],
            [6000, 1000, 1, 2000],
            [6000, 1000, 1, 3000],
            [6000, 1000, 1, 4000],
            [6000, 1000, 1, 5000],
            [6000, 1000, 1, 6000],
            [6000, 1000, 1, 7000],
            [6000, 1000, 1, 8000],
            [6000, 1000, 1, 9000]
        ];
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    [Trait("Category", "Performance")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    protected async Task FailureDetectionTimeTest_EnhancedMonitoringEnabled_Efm()
    {
        await this.FailureDetectionTimeTest_EnhancedMonitoringEnabled("efm");
    }

    protected async Task FailureDetectionTimeTest_EnhancedMonitoringEnabled(string efmPluginCode)
    {
        HostMonitorService.CloseAllMonitors();
        EfmPerformanceDataList.Clear();

        Logger.LogTrace($@"Test round with plugin: {efmPluginCode}");

        try
        {
            IEnumerable<int[]> parameters = GenerateFailureDetectionTimeParameters();
            foreach (int[] args in parameters)
            {
                try
                {
                    await this.Execute_FailureDetectionTimeTest_EnhancedMonitoringEnabled(
                        efmPluginCode, args[0], args[1], args[2], args[3]);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(ex.Message, ex);
                }
            }
        }
        finally
        {
            SheetsWriter.WritePerformanceDataToFile(@$"./EfmPerformanceResults-{CurrentDatabaseEngine}-{DateTime.Now.ToString("yyyyMMdd-HHmmss")}.xlsx", EfmPerformanceDataList);
            EfmPerformanceDataList.Clear();
        }
    }

    private async Task Execute_FailureDetectionTimeTest_EnhancedMonitoringEnabled(
        string pluginCode,
        int detectionTimeMs,
        int detectionIntervalMs,
        int detectionCount,
        int sleepDelayMs)
    {
        var instanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First();
        var connectionString =
            ConnectionStringHelper.GetUrl(Engine,
                instanceInfo.Host,
                instanceInfo.Port,
                Username,
                Password,
                TestEnvironment.Env.Info.ProxyDatabaseInfo.DefaultDbName,
                connectionTimeout: 3,
                commandTimeout: 70);
        connectionString += $";Plugins={pluginCode}";
        connectionString += $";FailureDetectionTime={detectionTimeMs}";
        connectionString += $";FailureDetectionInterval={detectionIntervalMs}";
        connectionString += $";FailureDetectionCount={detectionCount}";
        connectionString += $"Pooling=false;";

        PerformanceStatistics.PerformanceStatistics data = new();
        await this.MeasurePerformance(sleepDelayMs, RepeatTimes, connectionString, data, instanceInfo.InstanceId);
        data.ParameterDetectionTime = detectionTimeMs;
        data.ParameterDetectionInterval = detectionIntervalMs;
        data.ParameterDetectionCount = detectionCount;
        EfmPerformanceDataList.Enqueue(data);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "Manual")]
    [Trait("Category", "Performance")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task FailureDetectionTimeTest_FailoverAndEnhancedMonitoringEnabled()
    {
        HostMonitorService.CloseAllMonitors();
        FailoverAndEfmPerformanceDataList.Clear();

        try
        {
            IEnumerable<int[]> parameters = GenerateFailureDetectionTimeParameters();
            foreach (int[] args in parameters)
            {
                try
                {
                    await this.Execute_FailureDetectionTimeTest_FailoverAndEnhancedMonitoringEnabled(
                        "efm,failover", args[0], args[1], args[2], args[3]);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(ex.Message, ex);
                }
            }
        }
        finally
        {
            SheetsWriter.WritePerformanceDataToFile($@"./FailoverAndEfmPerformanceResults-{CurrentDatabaseEngine}-{DateTime.Now.ToString("yyyyMMdd-HHmmss")}.xlsx", FailoverAndEfmPerformanceDataList);
            FailoverAndEfmPerformanceDataList.Clear();
        }
    }

    private async Task Execute_FailureDetectionTimeTest_FailoverAndEnhancedMonitoringEnabled(
        string pluginCode,
        int detectionTimeMs,
        int detectionIntervalMs,
        int detectionCount,
        int sleepDelayMs)
    {
        var instanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First();
        var connectionString =
            ConnectionStringHelper.GetUrl(Engine, instanceInfo.Host, instanceInfo.Port, Username, Password, DefaultDbName, connectionTimeout: 3, commandTimeout: 80);
        connectionString += $";Plugins={pluginCode}";
        connectionString += $";FailureDetectionTime={detectionTimeMs}";
        connectionString += $";FailureDetectionInterval={detectionIntervalMs}";
        connectionString += $";FailureDetectionCount={detectionCount}";
        connectionString += ";FailoverTimeoutMs=120000";
        connectionString += ";FailoverMode=StrictReader";
        connectionString += $";ClusterInstanceHostPattern=?{TestEnvironment.Env.Info.ProxyDatabaseInfo!.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";
        connectionString += ";ClusterId=test-cluster-id";
        connectionString += $"Pooling=false;";

        PerformanceStatistics.PerformanceStatistics data = new();
        await this.MeasurePerformance(sleepDelayMs, RepeatTimes, connectionString, data, instanceInfo.InstanceId);
        data.ParameterDetectionTime = detectionTimeMs;
        data.ParameterDetectionInterval = detectionIntervalMs;
        data.ParameterDetectionCount = detectionCount;
        Logger.LogTrace($@"Collected data: {data}");
        FailoverAndEfmPerformanceDataList.Enqueue(data);
    }

    private async Task MeasurePerformance(
        int sleepDelayMs,
        int repeatTimes,
        string connectionString,
        PerformanceStatistics.PerformanceStatistics data,
        string instanceId)
    {
        await ProxyHelper.EnableAllConnectivityAsync();

        long[] elapsedTimeMs = new long[repeatTimes];

        for (int i = 0; i < repeatTimes; i++)
        {
            CancellationTokenSource cancellationToken = new();

            try
            {
                Logger.LogTrace("Resting 15s...");
                await Task.Delay(15000, cancellationToken.Token);
            }
            catch (OperationCanceledException)
            {
                Logger.LogTrace("Interrupted");
            }

            Logger.LogTrace(@$"Iteration: {i}");

            Stopwatch stopwatch = new();
            try
            {
                DbConnection connection = OpenConnectionWithRetry(connectionString);
                Logger.LogTrace(@"Connection is open.");
                using var command = connection.CreateCommand();
                command.CommandText = GetQuerySql(600);
                _ = Task.Run(async () =>
                    {
                        try
                        {
                            await Task.Delay(sleepDelayMs, cancellationToken.Token);
                            await ProxyHelper.DisableConnectivityAsync(instanceId);
                            stopwatch.Start();
                            Logger.LogTrace($@"Network outage started");
                        }
                        catch (OperationCanceledException)
                        {
                            Logger.LogTrace("Interrupted disable connectivity");
                        }
                    },
                    cancellationToken.Token);
                await command.ExecuteScalarAsync();
                Assert.Fail("Sleep query finished, should not be possible with network downed.");
            }
            catch (Exception ex)
            {
                Logger.LogTrace($@"Exception caught: {ex}");
                if (!stopwatch.IsRunning)
                {
                    Logger.LogTrace("Network outages start time is undefined!");
                    elapsedTimeMs[i] = 0;
                }
                else
                {
                    long failureTimeMs = stopwatch.ElapsedMilliseconds;
                    elapsedTimeMs[i] = failureTimeMs;
                }
            }
            finally
            {
                stopwatch.Stop();
                HostMonitorService.CloseAllMonitors();
                await cancellationToken.CancelAsync(); // Ensure task has stopped running
                await ProxyHelper.EnableAllConnectivityAsync();
            }
        }

        data.ParameterNetworkOutageDelayMs = sleepDelayMs;
        data.MinFailureDetectionTimeMs = elapsedTimeMs.Min();
        data.MaxFailureDetectionTimeMs = elapsedTimeMs.Max();
        data.AvgFailureDetectionTimeMs = (long)Math.Round(elapsedTimeMs.Average());
    }
}
