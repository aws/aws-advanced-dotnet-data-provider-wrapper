using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.Net;
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Tests.Container.Utils;
using MySqlConnector;
using Npgsql;
using Xunit;

namespace AwsWrapperDataProvider.Performance.Tests;

public class AdvancedPerformanceTests
{
    private static readonly AuroraTestUtils AuroraUtils = AuroraTestUtils.GetUtility();

    private static readonly int RepeatTimes =
        string.IsNullOrEmpty(Environment.GetEnvironmentVariable("REPEAT_TIMES"))
            ? 5
            : int.Parse(Environment.GetEnvironmentVariable("REPEAT_TIMES") ?? "5");

    private static readonly string DefaultDbName = TestEnvironment.Env.Info.DatabaseInfo.DefaultDbName;
    private static readonly string Username = TestEnvironment.Env.Info.DatabaseInfo.Username;
    private static readonly string Password = TestEnvironment.Env.Info.DatabaseInfo.Password;
    private static readonly DatabaseEngineDeployment Deployment = TestEnvironment.Env.Info.Request.Deployment;
    private static readonly DatabaseEngine Engine = TestEnvironment.Env.Info.Request.Engine;

    private static readonly ConcurrentQueue<PerformanceStatistics> PerformanceDataList = new();
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

    private const int EfmFailoverTimeoutMs = 300000;
    private const int EfmFailureDetectionTimeMs = 30000;
    private const int EfmFailureDetectionIntervalMs = 5000;
    private const int EfmFailureDetectionCount = 3;
    private const int NumberOfTasks = 5;
    private const string FileName = @"./AdvancedPerformanceResults.xlsx";

    private static string Query => Engine switch
    {
        DatabaseEngine.MYSQL => "SELECT SLEEP(600)",
        DatabaseEngine.PG => "SELECT pg_sleep(600)",
        _ => throw new NotSupportedException($"Unsupported engine: {Engine}"),
    };

    [Fact]
    [Trait("Category", "Performance")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task AdvancedPerformanceTest()
    {
        PerformanceDataList.Clear();

        try
        {
            IEnumerable<(int, int)> parameters = GenerateParameters();

            foreach ((int failoverDelayTimeMs, int runNumber) in parameters)
            {
                this.EnsureClusterHealthy();
                await this.EnsureDnsHealthy();

                Console.WriteLine($@"Iteration {runNumber}/{RepeatTimes} for {failoverDelayTimeMs}ms delay");

                await this.MeasurePerformance(failoverDelayTimeMs);
            }
        }
        finally
        {
            SheetsWriter.WritePerformanceDataToFile(FileName, PerformanceDataList);
            PerformanceDataList.Clear();
        }
    }

    private async Task MeasurePerformance(int sleepDelayMs)
    {
        AtomicLong downtimeNs = new();
        CountdownEvent startLatch = new(NumberOfTasks);
        CountdownEvent finishLatch = new(NumberOfTasks);
        CancellationTokenSource cancellationTokenSource = new();

        downtimeNs.Value = 0;

        _ = new[]
        {
            GetTask_Failover(sleepDelayMs, downtimeNs, startLatch, finishLatch, cancellationTokenSource.Token),
            GetTask_DirectDriver(sleepDelayMs, downtimeNs, startLatch, finishLatch, cancellationTokenSource.Token),
            GetTask_WrapperEfm(sleepDelayMs, downtimeNs, startLatch, finishLatch, cancellationTokenSource.Token),
            GetTask_WrapperInitialConnectionEfmFailover(sleepDelayMs, downtimeNs, startLatch, finishLatch, cancellationTokenSource.Token),
            GetTask_DNS(sleepDelayMs, downtimeNs, startLatch, finishLatch, cancellationTokenSource.Token),
        };

        Console.WriteLine(@"All tasks started.");

        Task timeoutTask = Task.Delay(TimeSpan.FromMinutes(5), cancellationTokenSource.Token);
        Task finishedTask = Task.Run(() => finishLatch.Wait(cancellationTokenSource.Token), cancellationTokenSource.Token);

        await Task.WhenAny(finishedTask, timeoutTask);

        Console.WriteLine(@"Performance tests is over.");

        Assert.True(downtimeNs.Value > 0);

        await cancellationTokenSource.CancelAsync();
    }

    private static IEnumerable<(int, int)> GenerateParameters()
    {
        int[] delays = [10000, 20000, 30000, 40000, 50000, 60000];

        foreach (int delay in delays)
        {
            for (int i = 1; i <= RepeatTimes; i++)
            {
                yield return (delay, i);
            }
        }
    }

    private async void EnsureClusterHealthy()
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

        Console.WriteLine($@"Writer is {dbInfo.Instances[0].InstanceId}");

        string writerIpAddress = (await Dns.GetHostAddressesAsync(writerHost))[0].ToString();
        Console.WriteLine($@"Writer resolves to {writerIpAddress}");
        Console.WriteLine($@"Cluster Endpoint is {clusterEndpoint}");

        Stopwatch stopwatch = Stopwatch.StartNew();
        string clusterIpAddress;

        do
        {
            clusterIpAddress = (await Dns.GetHostAddressesAsync(clusterEndpoint))[0].ToString();
            Console.WriteLine($@"Cluster Endpoint resolves to {clusterIpAddress}");

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
        AtomicLong downtimeNs,
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
                    startLatch.Signal(); // notify that this tasks is ready for work
                    startLatch.Wait(
                        TimeSpan.FromMinutes(5),
                        cancellationToken); // wait for other tasks to be ready

                    Console.WriteLine($@"Waiting {sleepDelayMs}ms...");
                    await Task.Delay(
                        sleepDelayMs,
                        cancellationToken);
                    Console.WriteLine(@"Trigger failover...");

                    // trigger failover
                    FailoverCluster();
                    downtimeNs.Value = Stopwatch.GetTimestamp();
                    Console.WriteLine($@"Downtime value is {(long)(downtimeNs.Value * 1000.0 / Stopwatch.Frequency)}ms");
                    Console.WriteLine(@"Failover is started.");
                }
                catch (OperationCanceledException exception)
                {
                    Console.WriteLine($@"Failover Operation Canceled Exception Exception: {exception}");
                }
                catch (Exception exception)
                {
                    Assert.Fail($"Failover task exception: {exception}");
                }
                finally
                {
                    finishLatch.Signal();
                    Console.WriteLine(@"Failover task is completed.");
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
        int sleepDelayMs,
        AtomicLong downtimeNs,
        CountdownEvent startLatch,
        CountdownEvent finishLatch,
        CancellationToken cancellationToken)
    {
        return Task.Run(
            async () =>
            {
                long failureTimeNano = 0;
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
                    Console.WriteLine(@"DirectDriver connection is open.");

                    await Task.Delay(
                        1000,
                        cancellationToken);
                    startLatch.Signal(); // notify that this task is ready for work
                    startLatch.Wait(
                        TimeSpan.FromMinutes(5),
                        cancellationToken); // wait for other tasks to be ready

                    Console.WriteLine(@"DirectDriver Starting long query...");

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
                        Console.WriteLine($@"DirectDriver task exception: {throwable}");
                        Console.WriteLine($@"Downtime: {downtimeNs.Value}");
                        Assert.True(downtimeNs.Value > 0);
                        Console.WriteLine($@"Timestamp: {Stopwatch.GetTimestamp()}");
                        failureTimeNano = Stopwatch.GetTimestamp() - downtimeNs.Value;
                    }
                }
                catch (OperationCanceledException exception)
                {
                    Console.WriteLine($@"DirectDriver Operation Canceled Exception Exception: {exception}");
                }
                catch (Exception exception)
                {
                    Assert.Fail($"DirectDriver task exception: {exception}");
                }
                finally
                {
                    conn?.Dispose();

                    var data = new PerformanceStatistics
                    {
                        ParameterFailoverDelayMs = sleepDelayMs, ParameterDriverName = $"DirectDriver - {TestEnvironment.Env.Info.DatabaseEngine}",
                    };
                    Console.WriteLine($@"Failure detection time is {failureTimeNano}ms");
                    data.FailureDetectionTimeMs = (long)(failureTimeNano * 1000.0 / Stopwatch.Frequency);

                    Console.WriteLine($@"DirectDriver Collected data: {data}");
                    PerformanceDataList.Enqueue(data);
                    Console.WriteLine($@"DirectDriver Failure detection time is {data.FailureDetectionTimeMs}ms");

                    finishLatch.Signal();
                    Console.WriteLine(@"DirectDriver task is completed.");
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
                Console.WriteLine($@"Connection attempt {connectCount + 1} failed: {ex.Message}");
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
        int sleepDelayMs,
        AtomicLong downtimeNs,
        CountdownEvent startLatch,
        CountdownEvent finishLatch,
        CancellationToken cancellationToken)
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
                Console.WriteLine(@"WrapperEfm connection is open.");

                await Task.Delay(1000, cancellationToken);
                startLatch.Signal(); // notify that this task is ready for work
                startLatch.Wait(TimeSpan.FromMinutes(5), cancellationToken); // wait for other tasks to be ready

                Console.WriteLine(@"WrapperEfm Starting long query...");

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
                    Console.WriteLine($@"WrapperEfm task exception: {throwable}");
                    Assert.True(downtimeNs.Value > 0);
                    failureTimeNano = Stopwatch.GetTimestamp() - downtimeNs.Value;
                }
            }
            catch (OperationCanceledException exception)
            {
                Console.WriteLine($@"WrapperEfm Operation Canceled Exception Exception: {exception}");
            }
            catch (Exception exception)
            {
                Assert.Fail($"WrapperEfm task exception: {exception}");
            }
            finally
            {
                conn?.Dispose();

                var data = new PerformanceStatistics
                {
                    ParameterFailoverDelayMs = sleepDelayMs, ParameterDriverName = $"AWS Wrapper ({TestEnvironment.Env.Info.DatabaseEngine}, EFM)",
                    FailureDetectionTimeMs = (long)(failureTimeNano * 1000.0 / Stopwatch.Frequency),
                };

                Console.WriteLine($@"WrapperEfm Collected data: {data}");
                PerformanceDataList.Enqueue(data);
                Console.WriteLine($@"WrapperEfm Failure detection time is {data.FailureDetectionTimeMs}ms");

                finishLatch.Signal();
                Console.WriteLine(@"WrapperEfm task is completed.");
            }
        }, cancellationToken);
    }

    private static Task GetTask_WrapperInitialConnectionEfmFailover(
        int sleepDelayMs,
        AtomicLong downtimeNs,
        CountdownEvent startLatch,
        CountdownEvent finishLatch,
        CancellationToken cancellationToken)
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
                Console.WriteLine(@"WrapperEfmFailover connection is open.");

                await Task.Delay(1000, cancellationToken);
                startLatch.Signal(); // notify that this task is ready for work
                startLatch.Wait(TimeSpan.FromMinutes(5), cancellationToken); // wait for other tasks to be ready

                Console.WriteLine(@"WrapperEfmFailover Starting long query...");

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
                    Console.WriteLine($@"WrapperEfmFailover task exception: {throwable}");
                    if (throwable.GetType().Name.Contains("FailoverSuccess"))
                    {
                        Assert.True(downtimeNs.Value > 0);
                        failureTimeNano = Stopwatch.GetTimestamp() - downtimeNs.Value;
                    }
                }
            }
            catch (OperationCanceledException exception)
            {
                Console.WriteLine($@"WrapperEfmFailover Operation Canceled Exception Exception: {exception}");
            }
            catch (Exception exception)
            {
                Assert.Fail($"WrapperEfmFailover task exception: {exception}");
            }
            finally
            {
                conn?.Dispose();

                var data = new PerformanceStatistics
                {
                    ParameterFailoverDelayMs = sleepDelayMs, ParameterDriverName = $"AWS Wrapper ({TestEnvironment.Env.Info.DatabaseEngine}, EFM, Failover)",
                    ReconnectTimeMs = (long)(failureTimeNano * 1000.0 / Stopwatch.Frequency),
                };

                Console.WriteLine($@"WrapperEfmFailover Collected data: {data}");
                PerformanceDataList.Enqueue(data);
                Console.WriteLine($@"WrapperEfmFailover Reconnect time is {data.ReconnectTimeMs}ms");

                finishLatch.Signal();
                Console.WriteLine(@"WrapperEfmFailover task is completed.");
            }
        }, cancellationToken);
    }

    private static Task GetTask_DNS(
        int sleepDelayMs,
        AtomicLong downtimeNs,
        CountdownEvent startLatch,
        CountdownEvent finishLatch,
        CancellationToken cancellationToken)
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
                Console.WriteLine($@"Cluster Endpoint resolves to {currentClusterIpAddress}");

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
                    Console.WriteLine($@"Cluster Endpoint resolves to {clusterIpAddress}");
                }

                if (!clusterIpAddress.Equals(currentClusterIpAddress))
                {
                    Assert.True(downtimeNs.Value > 0);
                    failureTimeNano = Stopwatch.GetTimestamp() - downtimeNs.Value;
                }
            }
            catch (OperationCanceledException exception)
            {
                Console.WriteLine($@"WrapperEfmFailover Operation Canceled Exception Exception: {exception}");
            }
            catch (Exception exception)
            {
                Assert.Fail($"DNS task exception: {exception}");
            }
            finally
            {
                var data = new PerformanceStatistics
                {
                    ParameterFailoverDelayMs = sleepDelayMs, ParameterDriverName = "DNS", DnsUpdateTimeMs = (long)(failureTimeNano * 1000.0 / Stopwatch.Frequency),
                };

                Console.WriteLine($@"DNS Collected data: {data}");
                PerformanceDataList.Enqueue(data);
                Console.WriteLine($@"DNS Update time is {data.DnsUpdateTimeMs}ms");

                finishLatch.Signal();
                Console.WriteLine(@"DNS task is completed.");
            }
        }, cancellationToken);
    }
}
