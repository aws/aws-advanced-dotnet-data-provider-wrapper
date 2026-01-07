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
using System.Data;
using System.Diagnostics;
using System.Net;
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using Npgsql;
using Xunit;

namespace AwsWrapperDataProvider.Performance.Tests;

public class AdvancedPerformanceTests
{
    private const int EfmFailoverTimeoutMs = 300000;
    private const int EfmFailureDetectionTimeMs = 30000;
    private const int EfmFailureDetectionIntervalMs = 5000;
    private const int EfmFailureDetectionCount = 3;
    private const int NumberOfTasks = 5;
    
    private static readonly ILogger<HostMonitoringPlugin> Logger = LoggerUtils.GetLogger<HostMonitoringPlugin>();

    private static readonly AuroraTestUtils AuroraUtils = AuroraTestUtils.GetUtility();

    private static readonly int RepeatTimes =
        string.IsNullOrEmpty(Environment.GetEnvironmentVariable("REPEAT_TIMES"))
            ? 3
            : int.Parse(Environment.GetEnvironmentVariable("REPEAT_TIMES") ?? "5");

    private static readonly string DefaultDbName = TestEnvironment.Env.Info.DatabaseInfo.DefaultDbName;
    private static readonly string Username = TestEnvironment.Env.Info.DatabaseInfo.Username;
    private static readonly string Password = TestEnvironment.Env.Info.DatabaseInfo.Password;
    private static readonly DatabaseEngineDeployment Deployment = TestEnvironment.Env.Info.Request.Deployment;
    private static readonly DatabaseEngine Engine = TestEnvironment.Env.Info.Request.Engine;
    private static readonly string CurrentDatabaseEngine = TestEnvironment.Env.Info.DatabaseEngine;

    private static readonly ConcurrentQueue<AdvancedPerformanceStatistics> PerformanceDataList = new();
    private static readonly string Endpoint = Deployment switch
    {
        DatabaseEngineDeployment.AURORA => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpoint,
        DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpoint,
        DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE => TestEnvironment.Env.Info.DatabaseInfo.Instances[0].Host,
        _ => throw new InvalidOperationException($"Unsupported deployment {Deployment}"),
    };

    private static readonly int Port = Deployment switch
    {
        DatabaseEngineDeployment.AURORA => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpointPort,
        DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpointPort,
        DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE => TestEnvironment.Env.Info.DatabaseInfo.Instances[0].Port,
        _ => throw new InvalidOperationException($"Unsupported deployment {Deployment}"),
    };

    private static readonly string FileName = $@"./AdvancedPerformanceResults-{CurrentDatabaseEngine}-{DateTime.Now.ToString("yyyyMMdd-HHmmss")}.xlsx";

    private static string Query => Engine switch
    {
        DatabaseEngine.MYSQL => "SELECT SLEEP(600)",
        DatabaseEngine.PG => "SELECT pg_sleep(600)",
        _ => throw new NotSupportedException($"Unsupported engine: {Engine}"),
    };

    private static int[] GenerateParameters()
    {
        return [10000, 20000, 30000, 40000, 50000, 60000];
    }

    [Fact]
    [Trait("Category", "Performance")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task AdvancedPerformanceTest()
    {
        PerformanceDataList.Clear();

        try
        {
            int[] parameters = GenerateParameters();

            foreach (int failoverDelayTimeMs in parameters)
            {
                List<List<long>> elapsedTimeMs =
                [
                    [],
                    [],
                    [],
                    []
                ];

                for (int runNumber = 0; runNumber < RepeatTimes; runNumber++)
                {
                    await this.EnsureClusterHealthy();
                    await this.EnsureDnsHealthy();

                    Logger.LogTrace($@"Iteration {runNumber}/{RepeatTimes} for {failoverDelayTimeMs}ms delay");

                    await this.MeasurePerformance(failoverDelayTimeMs, elapsedTimeMs);
                }

                PerformanceDataList.Enqueue(new AdvancedPerformanceStatistics
                {
                    ParameterFailoverDelayMs = failoverDelayTimeMs,
                    ParameterDriverName = $"DirectDriver - {TestEnvironment.Env.Info.DatabaseEngine}",
                    AvgFailureDetectionTimeMs = elapsedTimeMs[0].Average(),
                    MinFailureDetectionTimeMs = elapsedTimeMs[0].Min(),
                    MaxFailureDetectionTimeMs = elapsedTimeMs[0].Max(),
                });

                PerformanceDataList.Enqueue(new AdvancedPerformanceStatistics
                {
                    ParameterFailoverDelayMs = failoverDelayTimeMs,
                    ParameterDriverName = $"AWS Wrapper ({TestEnvironment.Env.Info.DatabaseEngine}, EFM)",
                    AvgFailureDetectionTimeMs = elapsedTimeMs[1].Average(),
                    MinFailureDetectionTimeMs = elapsedTimeMs[1].Min(),
                    MaxFailureDetectionTimeMs = elapsedTimeMs[1].Max(),
                });

                PerformanceDataList.Enqueue(new AdvancedPerformanceStatistics
                {
                    ParameterFailoverDelayMs = failoverDelayTimeMs,
                    ParameterDriverName = $"AWS Wrapper ({TestEnvironment.Env.Info.DatabaseEngine}, EFM, Failover)",
                    AvgReconnectTimeMs = elapsedTimeMs[2].Average(),
                    MinReconnectTimeMs = elapsedTimeMs[2].Min(),
                    MaxReconnectTimeMs = elapsedTimeMs[2].Max(),
                });

                PerformanceDataList.Enqueue(new AdvancedPerformanceStatistics
                {
                    ParameterFailoverDelayMs = failoverDelayTimeMs,
                    ParameterDriverName = "DNS",
                    AvgDnsUpdateTimeMs = elapsedTimeMs[3].Average(),
                    MinDnsUpdateTimeMs = elapsedTimeMs[3].Min(),
                    MaxDnsUpdateTimeMs = elapsedTimeMs[3].Max(),
                });
            }
        }
        finally
        {
            SheetsWriter.WriteAdvancedPerformanceDataToFile(FileName, PerformanceDataList);
            PerformanceDataList.Clear();
        }
    }

    private async Task MeasurePerformance(int sleepDelayMs, List<List<long>> elapsedTimes)
    {
        Stopwatch stopwatch = new();
        CountdownEvent startLatch = new(NumberOfTasks);
        CountdownEvent finishLatch = new(NumberOfTasks);
        CancellationTokenSource cancellationTokenSource = new();

        _ = new[]
        {
            GetTask_Failover(sleepDelayMs, stopwatch, startLatch, finishLatch, cancellationTokenSource.Token),
            GetTask_DirectDriver(stopwatch, startLatch, finishLatch, cancellationTokenSource.Token, elapsedTimes[0]),
            GetTask_WrapperEfm(stopwatch, startLatch, finishLatch, cancellationTokenSource.Token, elapsedTimes[1]),
            GetTask_WrapperInitialConnectionEfmFailover(stopwatch, startLatch, finishLatch, cancellationTokenSource.Token, elapsedTimes[2]),
            GetTask_DNS(stopwatch, startLatch, finishLatch, cancellationTokenSource.Token, elapsedTimes[3]),
        };

        Logger.LogTrace(@"All tasks started.");

        Task timeoutTask = Task.Delay(TimeSpan.FromMinutes(5), cancellationTokenSource.Token);
        Task finishedTask = Task.Run(() => finishLatch.Wait(cancellationTokenSource.Token), cancellationTokenSource.Token);

        await Task.WhenAny(finishedTask, timeoutTask);

        Logger.LogTrace(@"Performance tests is over.");

        Assert.True(stopwatch.ElapsedMilliseconds > 0);

        await cancellationTokenSource.CancelAsync();
    }

    private async Task EnsureClusterHealthy()
    {
        await AuroraUtils.WaitUntilClusterHasRightStateAsync(TestEnvironment.Env.Info.RdsDbName!);
        List<string> latestTopology = [];
        Stopwatch stopWatch = Stopwatch.StartNew();
        try
        {
            while ((latestTopology.Count != TestEnvironment.Env.Info.Request.NumOfInstances ||
                    !AuroraUtils.IsDBInstanceWriterAsync(latestTopology.First()).Result)
                   && stopWatch.Elapsed.Minutes < 5)
            {
                Thread.Sleep(5000);

                try
                {
                    latestTopology = AuroraUtils.GetAuroraInstanceIds();
                }
                catch (Exception)
                {
                    latestTopology = [];
                }
            }

            string currentWriter = latestTopology.First();
            Assert.True(AuroraUtils.IsDBInstanceWriterAsync(TestEnvironment.Env.Info.RdsDbName!, currentWriter).Result);
            TestEnvironment.Env.Info.DatabaseInfo.MoveInstanceFirst(currentWriter);
            TestEnvironment.Env.Info.ProxyDatabaseInfo!.MoveInstanceFirst(currentWriter);

            await AuroraUtils.MakeSureInstancesUpAsync(TimeSpan.FromSeconds(5));
            HostMonitorService.CloseAllMonitors();
        }
        finally
        {
            stopWatch.Stop();
            HostMonitorService.CloseAllMonitors();
        }
    }

    private async Task EnsureDnsHealthy()
    {
        TestDatabaseInfo dbInfo = TestEnvironment.Env.Info.DatabaseInfo;

        string writerHost = dbInfo.Instances[0].Host;
        string clusterEndpoint = dbInfo.ClusterEndpoint;

        string writerIpAddress = (await Dns.GetHostAddressesAsync(writerHost))[0].ToString();

        Stopwatch stopwatch = Stopwatch.StartNew();
        string clusterIpAddress;

        do
        {
            clusterIpAddress = (await Dns.GetHostAddressesAsync(clusterEndpoint))[0].ToString();

            if (!clusterIpAddress.Equals(writerIpAddress))
            {
                await Task.Delay(1000);
            }
        }
        while (!clusterIpAddress.Equals(writerIpAddress) && stopwatch.Elapsed.Minutes < 5);

        if (!clusterIpAddress.Equals(writerIpAddress))
        {
            Assert.Fail(@"DNS has stale data");
        }
    }

    private static Task GetTask_Failover(
        int sleepDelayMs,
        Stopwatch stopwatch,
        CountdownEvent startLatch,
        CountdownEvent finishLatch,
        CancellationToken cancellationToken)
    {
        return Task.Run(
            async () =>
            {
                try
                {
                    await Task.Delay(
                        1000,
                        cancellationToken);
                    startLatch.Signal();
                    startLatch.Wait(
                        TimeSpan.FromMinutes(5),
                        cancellationToken);

                    await Task.Delay(
                        sleepDelayMs,
                        cancellationToken);

                    FailoverCluster();
                    stopwatch.Start();
                    Logger.LogTrace(@"Failover is started.");
                }
                catch (OperationCanceledException exception)
                {
                    Logger.LogTrace($@"Failover Operation Canceled Exception Exception: {exception}");
                }
                catch (Exception exception)
                {
                    Assert.Fail($"Failover task exception: {exception}");
                }
                finally
                {
                    finishLatch.Signal();
                    Logger.LogTrace(@"Failover task is completed.");
                }
            },
            cancellationToken);
    }

    private static async void FailoverCluster()
    {
        string clusterId = TestEnvironment.Env.Info.RdsDbName!;
        string randomNode = AuroraUtils.GetRandomDBClusterReaderInstanceIdAsync(clusterId).Result;
        await AuroraUtils.FailoverClusterToTargetAsync(clusterId, randomNode);
    }

    private static Task GetTask_DirectDriver(
        Stopwatch stopwatch,
        CountdownEvent startLatch,
        CountdownEvent finishLatch,
        CancellationToken cancellationToken,
        List<long> elapsedTimesMs)
    {
        return Task.Run(
            async () =>
            {
                long failureTimeMs = 0;
                IDbConnection? conn = null;

                try
                {
                    var connectionString =
                        ConnectionStringHelper.GetUrl(
                            Engine,
                            Endpoint,
                            Port,
                            Username,
                            Password,
                            DefaultDbName);

                    conn = OpenConnectionWithRetry(connectionString);
                    Logger.LogTrace(@"DirectDriver connection is open.");

                    await Task.Delay(
                        1000,
                        cancellationToken);
                    startLatch.Signal();
                    startLatch.Wait(
                        TimeSpan.FromMinutes(5),
                        cancellationToken);

                    Logger.LogTrace(@"DirectDriver Starting long query...");

                    using var command = conn.CreateCommand();
                    command.CommandText = Query;
                    command.CommandTimeout = 700;

                    try
                    {
                        using var result = command.ExecuteReader();
                        Assert.Fail("Sleep query finished, should not be possible with network downed.");
                    }
                    catch (Exception throwable) when (throwable is not OperationCanceledException)
                    {
                        Logger.LogTrace($@"DirectDriver task exception: {throwable}");
                        Assert.True(stopwatch.ElapsedMilliseconds > 0);
                        failureTimeMs = stopwatch.ElapsedMilliseconds;
                    }
                }
                catch (OperationCanceledException exception)
                {
                    Logger.LogTrace($@"DirectDriver Operation Canceled Exception Exception: {exception}");
                }
                catch (Exception exception)
                {
                    Assert.Fail($"DirectDriver task exception: {exception}");
                }
                finally
                {
                    conn?.Dispose();
                    elapsedTimesMs.Add(failureTimeMs);
                    finishLatch.Signal();
                }
            },
            cancellationToken);
    }

    private static IDbConnection OpenConnectionWithRetry(string connectionString)
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
                Logger.LogTrace($@"Connection attempt {connectCount + 1} failed: {ex.Message}");
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


    private static Task GetTask_WrapperEfm(
        Stopwatch stopwatch,
        CountdownEvent startLatch,
        CountdownEvent finishLatch,
        CancellationToken cancellationToken,
        List<long> elapsedTimesMs)
    {
        return Task.Run(
            async () =>
        {
            long failureTimeNano = 0;
            IDbConnection? conn = null;

            try
            {
                var connectionString =
                    ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
                connectionString += ";Plugins=efm";
                connectionString += $";FailureDetectionTime={EfmFailureDetectionTimeMs}";
                connectionString += $";FailureDetectionInterval={EfmFailureDetectionIntervalMs}";
                connectionString += $";FailureDetectionCount={EfmFailureDetectionCount}";

                conn = OpenConnectionWithRetry(connectionString);
                Logger.LogTrace(@"WrapperEfm connection is open.");

                await Task.Delay(1000, cancellationToken);
                startLatch.Signal(); // notify that this task is ready for work
                startLatch.Wait(TimeSpan.FromMinutes(5), cancellationToken); // wait for other tasks to be ready

                Logger.LogTrace(@"WrapperEfm Starting long query...");

                using var command = conn.CreateCommand();
                command.CommandText = Query;
                command.CommandTimeout = 700;

                try
                {
                    using var result = command.ExecuteReader();
                    Assert.Fail("Sleep query finished, should not be possible with network downed.");
                }
                catch (Exception throwable) when (throwable is not OperationCanceledException)
                {
                    Logger.LogTrace($@"WrapperEfm task exception: {throwable}");
                    Assert.True(stopwatch.ElapsedMilliseconds > 0);
                    failureTimeNano = stopwatch.ElapsedMilliseconds;
                }
            }
            catch (OperationCanceledException exception)
            {
                Logger.LogTrace($@"WrapperEfm Operation Canceled Exception Exception: {exception}");
            }
            catch (Exception exception)
            {
                Assert.Fail($"WrapperEfm task exception: {exception}");
            }
            finally
            {
                conn?.Dispose();
                elapsedTimesMs.Add(failureTimeNano);
                finishLatch.Signal();
            }
        },
            cancellationToken);
    }

    private static Task GetTask_WrapperInitialConnectionEfmFailover(
        Stopwatch stopwatch,
        CountdownEvent startLatch,
        CountdownEvent finishLatch,
        CancellationToken cancellationToken,
        List<long> elapsedTimesMs)
    {
        return Task.Run(
            async () =>
        {
            long failureTimeNano = 0;
            IDbConnection? conn = null;

            try
            {
                var connectionString =
                    ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName);
                connectionString += ";Plugins=initialConnection,failover,efm";
                connectionString += $";FailureDetectionTime={EfmFailureDetectionTimeMs}";
                connectionString += $";FailureDetectionInterval={EfmFailureDetectionIntervalMs}";
                connectionString += $";FailureDetectionCount={EfmFailureDetectionCount}";
                connectionString += $";FailoverTimeoutMs={EfmFailoverTimeoutMs}";

                conn = OpenConnectionWithRetry(connectionString);
                Logger.LogTrace(@"WrapperEfmFailover connection is open.");

                await Task.Delay(1000, cancellationToken);
                startLatch.Signal(); // notify that this task is ready for work
                startLatch.Wait(TimeSpan.FromMinutes(5), cancellationToken); // wait for other tasks to be ready

                Logger.LogTrace(@"WrapperEfmFailover Starting long query...");

                using var command = conn.CreateCommand();
                command.CommandText = Query;
                command.CommandTimeout = 700;

                try
                {
                    using var result = command.ExecuteReader();
                    Assert.Fail("Sleep query finished, should not be possible with network downed.");
                }
                catch (Exception throwable) when (throwable is not OperationCanceledException)
                {
                    Logger.LogTrace($@"WrapperEfmFailover task exception: {throwable}");
                    if (throwable.GetType().Name.Contains("FailoverSuccess"))
                    {
                        Assert.True(stopwatch.ElapsedMilliseconds > 0);
                        failureTimeNano = stopwatch.ElapsedMilliseconds;
                    }
                }
            }
            catch (OperationCanceledException exception)
            {
                Logger.LogTrace($@"WrapperEfmFailover Operation Canceled Exception Exception: {exception}");
            }
            catch (Exception exception)
            {
                Assert.Fail($"WrapperEfmFailover task exception: {exception}");
            }
            finally
            {
                conn?.Dispose();
                elapsedTimesMs.Add(failureTimeNano);
                finishLatch.Signal();
                Logger.LogTrace(@"WrapperEfmFailover task is completed.");
            }
        },
            cancellationToken);
    }

    private static Task GetTask_DNS(
        Stopwatch stopwatch,
        CountdownEvent startLatch,
        CountdownEvent finishLatch,
        CancellationToken cancellationToken,
        List<long> elapsedTimesMs)
    {
        return Task.Run(
            async () =>
        {
            long failureTimeNano = 0;

            try
            {
                var clusterEndpoint = TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpoint;
                var addresses = await Dns.GetHostAddressesAsync(clusterEndpoint, cancellationToken);
                string currentClusterIpAddress = addresses[0].ToString();
                Logger.LogTrace($@"Cluster Endpoint resolves to {currentClusterIpAddress}");

                await Task.Delay(1000, cancellationToken);
                startLatch.Signal();
                startLatch.Wait(TimeSpan.FromMinutes(5), cancellationToken);

                addresses = await Dns.GetHostAddressesAsync(clusterEndpoint, cancellationToken);
                string clusterIpAddress = addresses[0].ToString();

                long startTimeNano = Stopwatch.GetTimestamp();
                while (clusterIpAddress.Equals(currentClusterIpAddress) &&
                       ((Stopwatch.GetTimestamp() - startTimeNano) * 1000.0 / Stopwatch.Frequency) <
                       600000)
                {
                    await Task.Delay(1000, cancellationToken);
                    addresses = await Dns.GetHostAddressesAsync(clusterEndpoint, cancellationToken);
                    clusterIpAddress = addresses[0].ToString();
                    Logger.LogTrace($@"Cluster Endpoint resolves to {clusterIpAddress}");
                }

                if (!clusterIpAddress.Equals(currentClusterIpAddress))
                {
                    Assert.True(stopwatch.ElapsedMilliseconds > 0);
                    failureTimeNano = stopwatch.ElapsedMilliseconds;
                }
            }
            catch (OperationCanceledException exception)
            {
                Logger.LogTrace($@"WrapperEfmFailover Operation Canceled Exception Exception: {exception}");
            }
            catch (Exception exception)
            {
                Assert.Fail($"DNS task exception: {exception}");
            }
            finally
            {
                elapsedTimesMs.Add(failureTimeNano);
                finishLatch.Signal();
                Logger.LogTrace(@"DNS task is completed.");
            }
        },
            cancellationToken);
    }
}
