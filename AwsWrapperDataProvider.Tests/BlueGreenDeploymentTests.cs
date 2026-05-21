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
using System.Data.Common;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Amazon.RDS.Model;
using AwsWrapperDataProvider.Driver.Plugins.BlueGreenConnection;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.Iam.Iam;
using AwsWrapperDataProvider.Tests.Container.Utils;
using AwsWrapperDataProvider.Tests.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using Npgsql;
using NPOI.Util;
using Xunit.Internal;

namespace AwsWrapperDataProvider.Tests;

public class BlueGreenDeploymentTests : IntegrationTestBase
{
    private const string DriverVersion = "1.1.0";

    private const string MysqlBgStatusQuery =
        "SELECT id, SUBSTRING_INDEX(endpoint, '.', 1) as hostId, endpoint, port, role, status, version FROM mysql.rds_topology";

    private const string PgAuroraBgStatusQuery =
        "SELECT id, SPLIT_PART(endpoint, '.', 1) as hostId, endpoint, port, role, status, version"
        + $" FROM pg_catalog.get_blue_green_fast_switchover_metadata('aws_advanced_dotnet_data_provider_wrapper-{DriverVersion}')";

    private static readonly string PgRdsBgStatusQuery =
        $"SELECT * FROM rds_tools.show_topology('aws_advanced_dotnet_data_provider_wrapper-{DriverVersion}')";

    private static readonly ILogger<BlueGreenDeploymentTests> Logger = LoggerUtils.GetLogger<BlueGreenDeploymentTests>();
    private static readonly AuroraTestUtils AuroraTestUtils = AuroraTestUtils.GetUtility();

    private static readonly bool IncludeClusterEndpoints = false;
    private static readonly bool IncludeWriterAndReaderOnly = false;

    private readonly ConcurrentDictionary<string, BlueGreenResults> results = new();
    private readonly ConcurrentQueue<Exception> unhandledExceptions = new();
    private readonly AtomicBool rollbackDetected = new(false);
    private string? rollbackDetails;
    private Stopwatch? stopwatch;

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-bgd")]
    [Trait("Database", "mysql-bgd")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-instance")]
    public void TestSwitchover()
    {
        this.unhandledExceptions.Clear();
        this.rollbackDetected.Set(false);
        this.rollbackDetails = null;

        this.stopwatch = Stopwatch.StartNew();

        CancellationTokenSource stopTokenSource = new();
        CancellationToken stopToken = stopTokenSource.Token;
        AtomicReference<CountdownEvent> startLatchAtomic = new(null);
        AtomicReference<CountdownEvent> finishLatchAtomic = new(null);

        List<Thread> threads = [];

        TestEnvironmentInfo info = TestEnvironment.Env.Info;
        TestInstanceInfo testInstance = info.DatabaseInfo.Instances[0];
        string dbName = info.DatabaseInfo.DefaultDbName;

        List<string> topologyInstances = this.GetBlueGreenEndpoints(info.BlueGreenDeploymentId);
        Logger.LogTrace("TopologyInstances: \n{Instances}", string.Join("\n", topologyInstances));

        foreach (string host in topologyInstances)
        {
            string hostId = host[..host.IndexOf('.')];
            Assert.NotNull(hostId);

            this.results[hostId] = new BlueGreenResults();
            BlueGreenResults hostResults = this.results[hostId];
            int port = testInstance.Port;

            if (RdsUtils.IsNotGreenAndOldPrefixInstance(host))
            {
                threads.Add(this.GetDirectTopologyMonitoringThread(
                    hostId, host, port, dbName, startLatchAtomic, stopToken, finishLatchAtomic, hostResults));
                threads.Add(this.GetDirectBlueConnectivityMonitoringThread(
                    hostId, host, port, dbName, startLatchAtomic, stopToken, finishLatchAtomic, hostResults));
                threads.Add(this.GetWrapperBlueExecutingConnectivityMonitoringThread(
                    hostId, host, port, dbName, startLatchAtomic, stopToken, finishLatchAtomic, hostResults));
                threads.Add(this.GetWrapperBlueNewConnectionMonitoringThread(
                    hostId, host, port, dbName, startLatchAtomic, stopToken, finishLatchAtomic, hostResults));
                threads.Add(this.GetWrapperBlueHostVerificationThread(
                    hostId, host, port, dbName, startLatchAtomic, stopToken, finishLatchAtomic, hostResults));
                threads.Add(this.GetBlueDnsMonitoringThread(
                    hostId, host, startLatchAtomic, stopToken, finishLatchAtomic, hostResults));
            }

            if (RdsUtils.IsGreenInstance(host))
            {
                threads.Add(this.GetDirectTopologyMonitoringThread(
                    hostId, host, port, dbName, startLatchAtomic, stopToken, finishLatchAtomic, hostResults));
                threads.Add(this.GetWrapperGreenConnectivityMonitoringThread(
                    hostId, host, port, dbName, startLatchAtomic, stopToken, finishLatchAtomic, hostResults));
                threads.Add(this.GetGreenDnsMonitoringThread(
                    hostId, host, startLatchAtomic, stopToken, finishLatchAtomic, hostResults));

                if (info.Request.Features.Contains(TestEnvironmentFeatures.IAM))
                {
                    threads.Add(this.GetGreenIamConnectivityMonitoringThread(
                        hostId, "BlueHostToken", RdsUtils.RemoveGreenInstancePrefix(host), host, port, dbName, startLatchAtomic, stopToken, finishLatchAtomic, hostResults, hostResults.GreenDirectIamIpWithBlueNodeConnectTimes, notifyOnFirstError: false, exitOnFirstSuccess: true));

                    threads.Add(this.GetGreenIamConnectivityMonitoringThread(
                        hostId, "GreenHostToken", host, host, port, dbName, startLatchAtomic, stopToken, finishLatchAtomic, hostResults, hostResults.GreenDirectIamIpWithGreenNodeConnectTimes, notifyOnFirstError: true, exitOnFirstSuccess: false));
                }
            }
        }

        threads.Add(this.GetBlueGreenSwitchoverTriggerThread(info.BlueGreenDeploymentId!, startLatchAtomic, finishLatchAtomic, this.results));
        threads.Add(this.GetRollbackDetectionThread(info.BlueGreenDeploymentId!, startLatchAtomic, stopToken, finishLatchAtomic));

        this.results.Values.ToList().ForEach(v => v.StartTime!.Set(0));

        CountdownEvent startEvent = new(threads.Count);
        CountdownEvent finishEvent = new(threads.Count);
        startLatchAtomic.Set(startEvent);
        finishLatchAtomic.Set(finishEvent);

        threads.ForEach(t => t.Start());
        Logger.LogTrace("All {ThreadsCount} threads started.", threads.Count);

        finishEvent.Wait(TimeSpan.FromMinutes(6), TestContext.Current.CancellationToken);
        Logger.LogTrace("All threads completed.");

        Thread.Sleep(TimeSpan.FromMinutes(3));

        Logger.LogTrace("Stopping all threads...");
        stopTokenSource.Cancel();
        Thread.Sleep(TimeSpan.FromSeconds(5));
        Logger.LogTrace("Interrupting all threads...");
        threads.ForEach(t => t.Interrupt());
        Thread.Sleep(TimeSpan.FromSeconds(5));

        Assert.True(this.results.All(x => x.Value.BgTriggerTime!.Get() > 0));

        Logger.LogTrace("Test is over.");
        this.PrintMetrics();

        if (!this.unhandledExceptions.IsEmpty)
        {
            this.LogUnhandledExceptions();
            Assert.Fail("There are unhandled exceptions.");
        }

        this.AssertTest();

        Logger.LogTrace("Completed");
    }

    private Thread GetDirectBlueConnectivityMonitoringThread(
        string hostId,
        string url,
        int port,
        string dbName,
        AtomicReference<CountdownEvent> startLatch,
        CancellationToken stopToken,
        AtomicReference<CountdownEvent> finishLatch,
        BlueGreenResults currentResults)
    {
        return new Thread(() =>
        {
            DbConnection? conn = null;
            try
            {
                conn = DirectOpenConnectionWithRetry(url, port, dbName);
                Logger.LogTrace("[DirectBlueConnectivity @ {HostId}] connection is open.", hostId);

                Thread.Sleep(1000);

                startLatch.Get()!.Signal();
                startLatch.Get()!.Wait(TimeSpan.FromMinutes(5));

                Logger.LogTrace("[DirectBlueConnectivity @ {HostId}] Starting connectivity monitoring.", hostId);

                while (!stopToken.IsCancellationRequested)
                {
                    try
                    {
                        using DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "SELECT 1";
                        cmd.ExecuteNonQuery();
                        Thread.Sleep(1000);
                    }
                    catch (Exception ex) when (ex is DbException or InvalidOperationException)
                    {
                        Logger.LogTrace("[DirectBlueConnectivity @ {HostId}] thread exception: {Error}", hostId, ex.Message);
                        currentResults.DirectBlueLostConnectionTime!.Set(this.stopwatch!.ElapsedMilliseconds);
                        break;
                    }
                }
            }
            catch (ThreadInterruptedException)
            {
                // Pass
            }
            catch (Exception exception)
            {
                Logger.LogTrace(exception, "[DirectBlueConnectivity @ {HostId}] thread unhandled exception", hostId);
                this.unhandledExceptions.Enqueue(exception);
            }
            finally
            {
                this.CloseConnection(conn);
                finishLatch.Get()!.Signal();
                Logger.LogTrace("[DirectBlueConnectivity @ {HostId}] thread is completed.", hostId);
            }
        });
    }

    private Thread GetWrapperBlueExecutingConnectivityMonitoringThread(
        string hostId,
        string url,
        int port,
        string dbName,
        AtomicReference<CountdownEvent> startLatch,
        CancellationToken stopToken,
        AtomicReference<CountdownEvent> finishLatch,
        BlueGreenResults currentResults)
    {
        return new Thread(() =>
        {
            AwsWrapperConnection? conn = null;
            BlueGreenConnectionPlugin bgPlugin;

            string query = TestEnvironment.Env.Info.Request.Engine switch
            {
                DatabaseEngine.MYSQL => "SELECT sleep(5)",
                DatabaseEngine.PG => "SELECT pg_catalog.pg_sleep(5)",
                _ => throw new NotSupportedException(TestEnvironment.Env.Info.Request.Engine.ToString()),
            };

            try
            {
                conn = OpenConnectionWithRetry(url, port, dbName, true);
                bgPlugin = conn.Unwrap<BlueGreenConnectionPlugin>();
                Assert.NotNull(bgPlugin);

                Logger.LogTrace("[WrapperBlueExecute @ {HostId}] connection is open.", hostId);

                Thread.Sleep(1000);

                startLatch.Get()!.Signal();
                startLatch.Get()!.Wait(TimeSpan.FromMinutes(5));

                Logger.LogTrace("[WrapperBlueExecute @ {HostId}] Starting connectivity monitoring.", hostId);

                // Phase 1: Execute until connection closes during switchover
                while (!stopToken.IsCancellationRequested)
                {
                    Stopwatch currentStopwatch = Stopwatch.StartNew();
                    long startTime = this.stopwatch!.ElapsedMilliseconds;
                    try
                    {
                        using DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = query;
                        cmd.ExecuteNonQuery();
                        currentResults.BlueWrapperPreSwitchoverExecuteTimes.Enqueue(
                            new TimeHolder(startTime, currentStopwatch.ElapsedMilliseconds, bgPlugin.GetHoldTimeMs()));
                    }
                    catch (Exception ex)
                    {
                        currentResults.BlueWrapperPreSwitchoverExecuteTimes.Enqueue(
                            new TimeHolder(startTime, currentStopwatch.ElapsedMilliseconds, bgPlugin.GetHoldTimeMs(), ex.Message));
                        if (IsConnectionClosed(conn))
                        {
                            break;
                        }
                    }

                    Thread.Sleep(1000);
                }

                Logger.LogTrace("[WrapperBlueExecute @ {HostId}] Connection closed, starting post-switchover phase.",
                    hostId);

                // Phase 2: Post-switchover - reconnect and continue executing
                while (!stopToken.IsCancellationRequested)
                {
                    long startTime = this.stopwatch!.ElapsedMilliseconds;
                    try
                    {
                        if (conn == null || IsConnectionClosed(conn))
                        {
                            conn = OpenConnection(url, port, dbName, true);
                            bgPlugin = conn.Unwrap<BlueGreenConnectionPlugin>();
                            Assert.NotNull(bgPlugin);
                            Logger.LogTrace("[WrapperBlueExecute @ {HostId}] Reconnected after switchover.", hostId);
                        }

                        Stopwatch currentStopwatch = Stopwatch.StartNew();
                        using DbCommand cmd = conn.CreateCommand();
                        cmd.CommandText = query;
                        cmd.ExecuteNonQuery();
                        currentResults.BlueWrapperPostSwitchoverExecuteTimes.Enqueue(
                            new TimeHolder(startTime, currentStopwatch.ElapsedMilliseconds, bgPlugin!.GetHoldTimeMs()));
                    }
                    catch (Exception ex) when (ex is DbException or InvalidOperationException or SocketException)
                    {
                        long holdTime = bgPlugin?.GetHoldTimeMs() ?? 0;
                        currentResults.BlueWrapperPostSwitchoverExecuteTimes.Enqueue(
                            new TimeHolder(startTime, 0, holdTime, ex.Message));
                        this.CloseConnection(conn);
                        conn = null;
                    }

                    Thread.Sleep(1000);
                }
            }
            catch (ThreadInterruptedException)
            {
                // Pass
            }
            catch (Exception exception)
            {
                Logger.LogTrace(exception, "[WrapperBlueExecute @ {HostId}] thread unhandled exception", hostId);
                this.unhandledExceptions.Enqueue(exception);
            }
            finally
            {
                this.CloseConnection(conn);
                finishLatch.Get()!.Signal();
                Logger.LogTrace("[WrapperBlueExecute @ {HostId}] thread is completed.", hostId);
            }
        });
    }

    private Thread GetWrapperBlueNewConnectionMonitoringThread(
        string hostId,
        string url,
        int port,
        string dbName,
        AtomicReference<CountdownEvent> startLatch,
        CancellationToken stopToken,
        AtomicReference<CountdownEvent> finishLatch,
        BlueGreenResults currentResults)
    {
        return new Thread(() =>
        {
            AwsWrapperConnection? conn = null;
            BlueGreenConnectionPlugin? bgPlugin = null;
            try
            {
                Thread.Sleep(1000);

                startLatch.Get()!.Signal();
                startLatch.Get()!.Wait(TimeSpan.FromMinutes(5));

                Logger.LogTrace("[WrapperBlueNewConnection @ {HostId}] Starting connectivity monitoring.", hostId);

                while (!stopToken.IsCancellationRequested)
                {
                    long startTime = this.stopwatch!.ElapsedMilliseconds;
                    Stopwatch currentStopwatch = Stopwatch.StartNew();
                    try
                    {
                        conn = OpenConnection(url, port, dbName, true);
                        bgPlugin = conn.Unwrap<BlueGreenConnectionPlugin>();
                        Assert.NotNull(bgPlugin);

                        currentResults.BlueWrapperConnectTimes.Enqueue(
                            new TimeHolder(startTime, currentStopwatch.ElapsedMilliseconds, bgPlugin!.GetHoldTimeMs()));
                    }
                    catch (TimeoutException ex)
                    {
                        Logger.LogTrace("[WrapperBlueNewConnection @ {HostId}] (TimeoutException) thread exception: {Error}", hostId, ex.Message);
                        RecordConnectionError(conn, currentResults, bgPlugin, startTime, currentStopwatch.ElapsedMilliseconds, ex.Message);
                    }
                    catch (Exception ex) when (ex is DbException or InvalidOperationException or SocketException)
                    {
                        Logger.LogTrace("[WrapperBlueNewConnection @ {HostId}] thread exception: {Error}", hostId, ex.Message);
                        RecordConnectionError(conn, currentResults, bgPlugin, startTime, currentStopwatch.ElapsedMilliseconds, ex.Message);
                    }

                    this.CloseConnection(conn);
                    conn = null;
                    Thread.Sleep(1000);
                }
            }
            catch (ThreadInterruptedException)
            {
                // Pass
            }
            catch (Exception exception)
            {
                Logger.LogTrace(exception, "[WrapperBlueNewConnection @ {HostId}] thread unhandled exception", hostId);
                this.unhandledExceptions.Enqueue(exception);
            }
            finally
            {
                this.CloseConnection(conn);
                finishLatch.Get()!.Signal();
                Logger.LogTrace("[WrapperBlueNewConnection @ {HostId}] thread is completed.", hostId);
            }
        });
    }

    private static void RecordConnectionError(
        AwsWrapperConnection? conn,
        BlueGreenResults results,
        BlueGreenConnectionPlugin? bgPlugin,
        long startTime,
        long elapsedTime,
        string errorMessage)
    {
        if (conn != null)
        {
            bgPlugin = conn.Unwrap<BlueGreenConnectionPlugin>();
            Assert.NotNull(bgPlugin);
            results.BlueWrapperConnectTimes.Enqueue(
                new TimeHolder(startTime, elapsedTime, bgPlugin!.GetHoldTimeMs(), errorMessage));
        }
        else
        {
            results.BlueWrapperConnectTimes.Enqueue(
                new TimeHolder(startTime, elapsedTime, errorMessage));
        }
    }

    private Thread GetWrapperBlueHostVerificationThread(
        string hostId,
        string url,
        int port,
        string dbName,
        AtomicReference<CountdownEvent> startLatch,
        CancellationToken stopToken,
        AtomicReference<CountdownEvent> finishLatch,
        BlueGreenResults results)
    {
        return new Thread(() =>
        {
            DbConnection? conn = null;
            string? originalBlueIp;
            try
            {
                conn = OpenConnectionWithRetry(url, port, dbName, true);
                originalBlueIp = this.GetConnectedServerHost(conn);
                this.CloseConnection(conn);
                conn = null;

                if (originalBlueIp == null)
                {
                    throw new InvalidOperationException("Failed to get original blue IP from initial connection");
                }

                Logger.LogTrace("[WrapperBlueHostVerification @ {HostId}] Captured original blue IP: {Ip}", hostId, originalBlueIp);

                Thread.Sleep(1000);

                startLatch.Get()!.Signal();
                startLatch.Get()!.Wait(TimeSpan.FromMinutes(5));

                Logger.LogTrace("[WrapperBlueHostVerification @ {HostId}] Starting host verification. Original blue IP: {Ip}", hostId, originalBlueIp);

                string? capturedOriginalBlueIp = originalBlueIp;
                while (!stopToken.IsCancellationRequested)
                {
                    long timestamp = this.stopwatch!.ElapsedMilliseconds;
                    try
                    {
                        conn = OpenConnection(url, port, dbName, true);
                        string connectedHost = this.GetConnectedServerHost(conn) ?? string.Empty;
                        timestamp = this.stopwatch!.ElapsedMilliseconds;
                        HostVerificationResult result = HostVerificationResult.Success(timestamp, connectedHost, capturedOriginalBlueIp);
                        results.HostVerificationResults.Enqueue(result);

                        if (result.ConnectedToBlue)
                        {
                            Logger.LogTrace("[WrapperBlueHostVerification @ {HostId}] Connected to blue cluster! Host: {Host}", hostId, connectedHost);
                        }
                        else
                        {
                            Logger.LogTrace("[WrapperBlueHostVerification @ {HostId}] Connected to green! Host: {Host}", hostId, connectedHost);
                        }
                    }
                    catch (Exception ex) when (ex is DbException or InvalidOperationException or SocketException)
                    {
                        Logger.LogTrace("[WrapperBlueHostVerification @ {HostId}] thread exception: {Error}", hostId, ex.Message);
                        results.HostVerificationResults.Enqueue(
                            HostVerificationResult.Failure(timestamp, capturedOriginalBlueIp, ex.Message));
                    }

                    this.CloseConnection(conn);
                    conn = null;
                    Thread.Sleep(1000);
                }
            }
            catch (ThreadInterruptedException)
            {
                // Pass
            }
            catch (Exception exception)
            {
                Logger.LogTrace(exception, "[WrapperBlueHostVerification @ {HostId}] thread unhandled exception", hostId);
                this.unhandledExceptions.Enqueue(exception);
            }
            finally
            {
                this.CloseConnection(conn);
                finishLatch.Get()!.Signal();
                Logger.LogTrace("[WrapperBlueHostVerification @ {HostId}] thread is completed.", hostId);
            }
        });
    }

    private string? GetConnectedServerHost(DbConnection conn)
    {
        using DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = DriverHelper.GetHostnameSql();
        var result = cmd.ExecuteScalar();
        return result?.ToString();
    }

    private Thread GetGreenDnsMonitoringThread(
        string hostId,
        string host,
        AtomicReference<CountdownEvent> startLatch,
        CancellationToken stopToken,
        AtomicReference<CountdownEvent> finishLatch,
        BlueGreenResults currentResults)
    {
        return new Thread(() =>
        {
            try
            {
                startLatch.Get()!.Signal();
                startLatch.Get()!.Wait(TimeSpan.FromMinutes(5));

                var ip = Dns.GetHostAddresses(host)[0].ToString();
                Logger.LogTrace("[GreenDNS @ {HostId}] {Host} -> {Ip}", hostId, host, ip);

                while (!stopToken.IsCancellationRequested)
                {
                    Thread.Sleep(1000);
                    try
                    {
                        Dns.GetHostAddresses(host);
                    }
                    catch (SocketException)
                    {
                        currentResults.DnsGreenRemovedTime!.Set(this.stopwatch!.ElapsedMilliseconds);
                        break;
                    }
                }
            }
            catch (ThreadInterruptedException)
            {
                // Pass
            }
            catch (Exception e)
            {
                Logger.LogTrace(e, "[GreenDNS @ {HostId}] thread unhandled exception", hostId);
                this.unhandledExceptions.Enqueue(e);
            }
            finally
            {
                finishLatch.Get()!.Signal();
                Logger.LogTrace("[GreenDNS @ {HostId}] thread is completed.", hostId);
            }
        });
    }

    public Thread GetBlueDnsMonitoringThread(
        string hostId,
        string host,
        AtomicReference<CountdownEvent> startLatch,
        CancellationToken stopToken,
        AtomicReference<CountdownEvent> finishLatch,
        BlueGreenResults currentResults)
    {
        return new Thread(() =>
        {
            try
            {
                startLatch.Get()!.Signal();
                startLatch.Get()!.Wait(TimeSpan.FromMinutes(5));

                var originalIp = Dns.GetHostAddresses(host)[0].ToString();
                Logger.LogTrace("[BlueDNS @ {HostId}] {Host} -> {Ip}", hostId, host, originalIp);

                while (!stopToken.IsCancellationRequested)
                {
                    Thread.Sleep(1000);
                    try
                    {
                        var currentIp = Dns.GetHostAddresses(host)[0].ToString();
                        if (!currentIp.Equals(originalIp))
                        {
                            currentResults.DnsBlueChangedTime!.Set(this.stopwatch!.ElapsedMilliseconds);
                            Logger.LogTrace("[BlueDNS @ {HostId}] {Host} -> {Ip}", hostId, host, currentIp);
                            break;
                        }
                    }
                    catch (SocketException ex)
                    {
                        Logger.LogTrace("[BlueDNS @ {HostId}] Error: {Error}", hostId, ex.Message);
                        currentResults.DnsBlueError = ex.Message;
                        currentResults.DnsBlueChangedTime!.Set(this.stopwatch!.ElapsedMilliseconds);
                        break;
                    }
                }
            }
            catch (ThreadInterruptedException)
            {
                // Pass
            }
            catch (Exception e)
            {
                Logger.LogTrace(e, "[BlueDNS @ {HostId}] thread unhandled exception", hostId);
                this.unhandledExceptions.Enqueue(e);
            }
            finally
            {
                finishLatch.Get()!.Signal();
                Logger.LogTrace("[BlueDNS @ {HostId}] thread is completed.", hostId);
            }
        });
    }

    private Thread GetDirectTopologyMonitoringThread(
        string hostId,
        string url,
        int port,
        string dbName,
        AtomicReference<CountdownEvent> startLatch,
        CancellationToken stopToken,
        AtomicReference<CountdownEvent> finishLatch,
        BlueGreenResults currentResults)
    {
        return new Thread(() =>
        {
            DbConnection? conn = null;

            string query = TestEnvironment.Env.Info.Request.Engine switch
            {
                DatabaseEngine.MYSQL => MysqlBgStatusQuery,
                DatabaseEngine.PG => TestEnvironment.Env.Info.Request.Deployment switch
                {
                    DatabaseEngineDeployment.AURORA => PgAuroraBgStatusQuery,
                    DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE => PgRdsBgStatusQuery,
                    _ => throw new NotSupportedException(TestEnvironment.Env.Info.Request.Deployment.ToString()),
                },
                _ => throw new NotSupportedException(TestEnvironment.Env.Info.Request.Engine.ToString()),
            };

            try
            {
                conn = DirectOpenConnectionWithRetry(url, port, dbName);
                Logger.LogTrace("[DirectTopology @ {HostId}] connection opened", hostId);

                Thread.Sleep(1000);

                startLatch.Get()!.Signal();
                startLatch.Get()!.Wait(TimeSpan.FromMinutes(5));

                Logger.LogTrace("[DirectTopology @ {HostId}] Starting BG statuses monitoring.", hostId);

                var deadline = DateTime.UtcNow.AddMinutes(15);

                while (!stopToken.IsCancellationRequested && DateTime.UtcNow < deadline)
                {
                    if (conn == null)
                    {
                        conn = DirectOpenConnectionWithRetry(url, port, dbName);
                        Logger.LogTrace("[DirectTopology @ {HostId}] connection re-opened", hostId);
                    }

                    try
                    {
                        using var cmd = conn.CreateCommand();
                        cmd.CommandText = query;
                        using var reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            var queryRole = reader.GetString(reader.GetOrdinal("role"));
                            var queryVersion = reader.GetString(reader.GetOrdinal("version"));
                            var queryNewStatus = reader.GetString(reader.GetOrdinal("status"));
                            bool isGreen = BlueGreenRole.ParseRole(queryRole, queryVersion) == BlueGreenRoleType.TARGET;

                            var statusDict = isGreen ? currentResults.GreenStatusTime : currentResults.BlueStatusTime;
                            statusDict.GetOrAdd(queryNewStatus, _ =>
                            {
                                Logger.LogTrace("[DirectTopology @ {HostId}] status changed to: {Status}", hostId, queryNewStatus);
                                return this.stopwatch!.ElapsedMilliseconds;
                            });
                        }

                        Thread.Sleep(100);
                    }
                    catch (Exception ex) when (ex is DbException or InvalidOperationException or SocketException)
                    {
                        Logger.LogTrace(ex, "[DirectTopology @ {HostId}] thread exception: {Error}", hostId, ex.Message);
                        this.CloseConnection(conn);
                        conn = null;
                    }
                }
            }
            catch (ThreadInterruptedException)
            {
                // Pass
            }
            catch (Exception exception)
            {
                Logger.LogTrace(exception, "[DirectTopology @ {HostId}] thread unhandled exception", hostId);
                this.unhandledExceptions.Enqueue(exception);
            }
            finally
            {
                this.CloseConnection(conn);
                finishLatch.Get()!.Signal();
                Logger.LogTrace("[DirectTopology @ {HostId}] thread is completed.", hostId);
            }
        });
    }

    private Thread GetWrapperGreenConnectivityMonitoringThread(
        string hostId,
        string url,
        int port,
        string dbName,
        AtomicReference<CountdownEvent> startLatch,
        CancellationToken stopToken,
        AtomicReference<CountdownEvent> finishLatch,
        BlueGreenResults currentResults)
    {
        return new Thread(() =>
        {
            AwsWrapperConnection? conn = null;
            try
            {
                conn = OpenConnectionWithRetry(url, port, dbName, true);
                Logger.LogTrace("[WrapperGreenConnectivity @ {HostId}] connection is open.", hostId);

                var bgPlugin = conn.Unwrap<BlueGreenConnectionPlugin>();
                Assert.NotNull(bgPlugin);

                Thread.Sleep(1000);

                startLatch.Get()!.Signal();
                startLatch.Get()!.Wait(TimeSpan.FromMinutes(5));

                Logger.LogTrace("[WrapperGreenConnectivity @ {HostId}] Starting connectivity monitoring.", hostId);

                Stopwatch currentStopwatch = Stopwatch.StartNew();
                while (!stopToken.IsCancellationRequested)
                {
                    long startTime = this.stopwatch!.ElapsedMilliseconds;
                    try
                    {
                        using var cmd = conn.CreateCommand();
                        cmd.CommandText = "SELECT 1";
                        currentStopwatch = Stopwatch.StartNew();
                        cmd.ExecuteNonQuery();
                        currentResults.GreenWrapperExecuteTimes.Enqueue(
                            new TimeHolder(startTime, currentStopwatch.ElapsedMilliseconds, bgPlugin!.GetHoldTimeMs()));
                        Thread.Sleep(1000);
                    }
                    catch (TimeoutException ex)
                    {
                        Logger.LogTrace("[WrapperGreenConnectivity @ {HostId}] (TimeoutException) thread exception: {Error}", hostId, ex.Message);
                        currentResults.GreenWrapperExecuteTimes.Enqueue(
                            new TimeHolder(startTime, currentStopwatch.ElapsedMilliseconds, bgPlugin!.GetHoldTimeMs(), ex.Message));
                        if (IsConnectionClosed(conn))
                        {
                            currentResults.WrapperGreenLostConnectionTime!.Set(this.stopwatch!.ElapsedMilliseconds);
                            break;
                        }
                    }
                    catch (Exception ex) when (ex is DbException or InvalidOperationException)
                    {
                        Logger.LogTrace("[WrapperGreenConnectivity @ {HostId}] thread exception: {Error}", hostId, ex.Message);
                        currentResults.WrapperGreenLostConnectionTime!.Set(this.stopwatch!.ElapsedMilliseconds);
                        break;
                    }
                }
            }
            catch (ThreadInterruptedException)
            {
                // Pass
            }
            catch (Exception exception)
            {
                Logger.LogTrace(exception, "[WrapperGreenConnectivity @ {HostId}] thread unhandled exception", hostId);
                this.unhandledExceptions.Enqueue(exception);
            }
            finally
            {
                this.CloseConnection(conn);
                finishLatch.Get()!.Signal();
                Logger.LogTrace("[WrapperGreenConnectivity @ {HostId}] thread is completed.", hostId);
            }
        });
    }

    private Thread GetBlueGreenSwitchoverTriggerThread(
        string blueGreenId,
        AtomicReference<CountdownEvent> startLatch,
        AtomicReference<CountdownEvent> finishLatch,
        ConcurrentDictionary<string, BlueGreenResults> currentResults)
    {
        return new Thread(() =>
        {
            try
            {
                startLatch.Get()!.Signal();
                startLatch.Get()!.Wait(TimeSpan.FromMinutes(5));

                Logger.LogTrace("[Switchover] thread is started.");
                long timeMs = this.stopwatch!.ElapsedMilliseconds;
                foreach ((_, BlueGreenResults value) in currentResults)
                    value.ThreadsSyncTime!.Set(timeMs);

                Thread.Sleep(TimeSpan.FromSeconds(30));
                Logger.LogTrace("[Switchover] Switchover requested");
                _ = AuroraTestUtils.SwitchoverBlueGreenDeployment(blueGreenId);
                Thread.Sleep(TimeSpan.FromSeconds(1));
                timeMs = this.stopwatch!.ElapsedMilliseconds;
                foreach ((_, BlueGreenResults value) in currentResults)
                    value.BgTriggerTime!.Set(timeMs);
            }
            catch (ThreadInterruptedException)
            {
                // Pass
            }
            catch (Exception exception)
            {
                Logger.LogTrace(exception, "[Switchover] thread unhandled exception");
                this.unhandledExceptions.Enqueue(exception);
            }
            finally
            {
                finishLatch.Get()!.Signal();
                Logger.LogTrace("[Switchover] thread is completed.");
            }
        });
    }

    // Green node
    // Check: connectivity (opening a new connection) with IAM when using node IP address
    // Expect: lose connectivity after green node changes its name (green prefix to no-prefix)
    // Can terminate for itself
    private Thread GetGreenIamConnectivityMonitoringThread(
        string hostId,
        string threadPrefix,
        string iamTokenHost,
        string connectHost,
        int port,
        string dbName,
        AtomicReference<CountdownEvent> startLatch,
        CancellationToken stopToken,
        AtomicReference<CountdownEvent> finishLatch,
        BlueGreenResults currentResults,
        ConcurrentQueue<TimeHolder> resultQueue,
        bool notifyOnFirstError,
        bool exitOnFirstSuccess)
    {
        return new Thread(() =>
        {
            DbConnection? conn = null;
            try
            {
                var iamTokenUtility = new IamTokenUtility();
                var info = TestEnvironment.Env.Info;
                string greenNodeConnectIp = Dns.GetHostAddresses(connectHost).First().ToString();

                Thread.Sleep(1000);

                startLatch.Get()!.Signal();
                startLatch.Get()!.Wait(TimeSpan.FromMinutes(5));

                Logger.LogTrace(
                    "[DirectGreenIamIp{ThreadPrefix} @ {HostId}] Starting connectivity monitoring {IamTokenHost}", threadPrefix, hostId, iamTokenHost);

                while (!stopToken.IsCancellationRequested)
                {
                    string token = iamTokenUtility.GenerateAuthenticationTokenAsync(info.Region!, iamTokenHost, this.GetPort(), info.IamUsername!).GetAwaiter().GetResult();

                    string connectionString = ConnectionStringHelper.GetUrl(
                        Engine, greenNodeConnectIp, port, info.IamUsername, token, dbName, 10, 10, null, false);

                    long startTime = this.stopwatch!.ElapsedMilliseconds;
                    try
                    {
                        conn = DriverHelper.CreateUnopenedConnection(Engine, connectionString);
                        conn.Open();
                        long elapsedTime = this.stopwatch.ElapsedMilliseconds - startTime;
                        resultQueue.Enqueue(new TimeHolder(startTime, elapsedTime));

                        if (exitOnFirstSuccess)
                        {
                            currentResults.GreenNodeChangeNameTime!.CompareAndSet(0, this.stopwatch.ElapsedMilliseconds);
                            Logger.LogTrace(
                                "[DirectGreenIamIp{ThreadPrefix} @ {HostId}] Successfully connected. Exiting thread...", threadPrefix, hostId);
                            return;
                        }
                    }
                    catch (Exception ex) when (ex is TimeoutException)
                    {
                        long elapsedTime = this.stopwatch.ElapsedMilliseconds - startTime;
                        Logger.LogTrace(
                            "[DirectGreenIamIp{ThreadPrefix} @ {HostId}] (TimeoutException) thread exception: {Error}", threadPrefix, hostId, ex.Message);
                        resultQueue.Enqueue(new TimeHolder(startTime, elapsedTime, ex.Message));
                    }
                    catch (Exception ex) when (ex is DbException)
                    {
                        long elapsedTime = this.stopwatch.ElapsedMilliseconds - startTime;
                        Logger.LogTrace(
                            "[DirectGreenIamIp{ThreadPrefix} @ {HostId}] thread exception: {Error}", threadPrefix, hostId, ex.Message);
                        resultQueue.Enqueue(new TimeHolder(startTime, elapsedTime, ex.Message));

                        if (notifyOnFirstError
                            && (ex.Message.Contains("Access denied") || ex.Message.Contains("PAM authentication failed")))
                        {
                            currentResults.GreenNodeChangeNameTime!.CompareAndSet(0, this.stopwatch.ElapsedMilliseconds);
                            Logger.LogTrace(
                                "[DirectGreenIamIp{ThreadPrefix} @ {HostId}] The first authentication failure exception. Exiting thread...", threadPrefix, hostId);
                            return;
                        }
                    }

                    this.CloseConnection(conn);
                    conn = null;
                    Thread.Sleep(1000);
                }
            }
            catch (Exception exception) when (exception is not ThreadInterruptedException)
            {
                Logger.LogTrace(exception, "[DirectGreenIamIp{ThreadPrefix} @ {HostId}] thread unhandled exception", threadPrefix, hostId);
                this.unhandledExceptions.Enqueue(exception);
            }
            finally
            {
                this.CloseConnection(conn);
                finishLatch.Get()!.Signal();
                Logger.LogTrace("[DirectGreenIamIp{ThreadPrefix} @ {HostId}] thread is completed.", threadPrefix, hostId);
            }
        });
    }

    private static string GetConnectionString(string url, int port, string dbName, bool bgPlugin)
    {
        TestEnvironmentInfo info = TestEnvironment.Env.Info;
        string connectionString =
            ConnectionStringHelper.GetUrl(
                Engine,
                url,
                port,
                info.Request.Features.Contains(TestEnvironmentFeatures.IAM) ? info.IamUsername : Username,
                info.Request.Features.Contains(TestEnvironmentFeatures.IAM) ? null : Password,
                dbName,
                10,
                10,
                bgPlugin ? GetWrapperConnectionPlugins() : GetDefaultConnectionPlugins(),
                false);

        if (info.Request.Features.Contains(TestEnvironmentFeatures.IAM))
        {
            connectionString += $";IamRegion={info.Region}";
        }

        if (bgPlugin)
        {
            connectionString += $";BgdId={info.BlueGreenDeploymentId}";
        }

        return connectionString;
    }

    private static AwsWrapperConnection OpenConnectionWithRetry(string url, int port, string dbName, bool bgPlugin)
    {
        string connectionString = GetConnectionString(url, port, dbName, bgPlugin);
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
                Logger.LogError(ex, $@"Connection attempt {connectCount + 1} failed: {ex.Message}");
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

    private static AwsWrapperConnection OpenConnection(string url, int port, string dbName, bool bgPlugin)
    {
        string connectionString = GetConnectionString(url, port, dbName, bgPlugin);
        AwsWrapperConnection? connection = null;

        connection = Engine switch
        {
            DatabaseEngine.MYSQL => new AwsWrapperConnection<MySqlConnection>(connectionString),
            DatabaseEngine.PG => new AwsWrapperConnection<NpgsqlConnection>(connectionString),
            _ => throw new NotSupportedException($"Unsupported engine: {Engine}"),
        };

        connection.Open();
        return connection;
    }

    private static DbConnection DirectOpenConnectionWithRetry(string url, int port, string dbName)
    {
        string connectionString = ConnectionStringHelper.GetUrl(
            Engine, url, port, Username, Password, dbName, 10, 10, null, false);
        DbConnection? connection = null;
        int connectCount = 0;

        while (connection == null && connectCount < 10)
        {
            try
            {
                connection = DriverHelper.CreateUnopenedConnection(Engine, connectionString);
                connection.Open();
                return connection;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, $@"Direct connection attempt {connectCount + 1} failed: {ex.Message}");
                connection?.Dispose();
                connection = null;
                connectCount++;

                if (connectCount < 10)
                {
                    Thread.Sleep(1000);
                }
            }
        }

        throw new Exception($@"Failed to establish direct connection after {connectCount} attempts");
    }

    private void CloseConnection(DbConnection? conn)
    {
        try
        {
            conn?.Dispose();
        }
        catch
        {
            // ignored
        }
    }

    private Thread GetRollbackDetectionThread(
        string bgdId,
        AtomicReference<CountdownEvent> startLatch,
        CancellationToken stopToken,
        AtomicReference<CountdownEvent> finishLatch)
    {
        return new Thread(() =>
        {
            try
            {
                startLatch.Get()!.Signal();
                startLatch.Get()!.Wait(TimeSpan.FromMinutes(5));

                Logger.LogTrace("[RollbackDetection] Starting rollback monitoring for id: {BgdId}", bgdId);

                var highestPhaseSeen = BlueGreenPhaseType.NOT_CREATED;

                while (!stopToken.IsCancellationRequested)
                {
                    var status = BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(bgdId);

                    if (status?.CurrentPhase != null)
                    {
                        var currentPhase = status.CurrentPhase;

                        if (currentPhase == BlueGreenPhaseType.CREATED
                            && highestPhaseSeen >= BlueGreenPhaseType.PREPARATION
                            && highestPhaseSeen != BlueGreenPhaseType.COMPLETED)
                        {
                            this.rollbackDetected.Set(true);
                            this.rollbackDetails = $"Rollback detected: phase regressed from {highestPhaseSeen} to CREATED";
                            Logger.LogWarning("{Details}", this.rollbackDetails);
                            break;
                        }

                        if (currentPhase > highestPhaseSeen)
                        {
                            highestPhaseSeen = currentPhase;
                            Logger.LogTrace("[RollbackDetection] Phase advanced to: {Phase}", highestPhaseSeen);
                        }
                    }

                    Thread.Sleep(100);
                }
            }
            catch (Exception e)
            {
                Logger.LogTrace(e, "[RollbackDetection] thread unhandled exception");
                this.unhandledExceptions.Enqueue(e);
            }
            finally
            {
                finishLatch.Get()!.Signal();
                Logger.LogTrace("[RollbackDetection] thread is completed.");
            }
        });
    }

    private List<string> GetBlueGreenEndpoints(string? bgId)
    {
        if (bgId == null)
        {
            return [];
        }

        BlueGreenDeployment? blueGreenDeployment = AuroraTestUtils.GetBlueGreenDeployment(bgId);
        if (blueGreenDeployment == null)
        {
            Logger.LogTrace(@"BG not found" + bgId);
            return [];
        }

        Logger.LogTrace(System.Text.Json.JsonSerializer.Serialize(blueGreenDeployment));

        Logger.LogTrace($"ID: {blueGreenDeployment.BlueGreenDeploymentIdentifier}");
        Logger.LogTrace($"Source: {blueGreenDeployment.Source}");
        Logger.LogTrace($"Target: {blueGreenDeployment.Target}");
        Logger.LogTrace($"Status: {blueGreenDeployment.Status}");

        switch (TestEnvironment.Env.Info.Request.Deployment)
        {
            case DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE:
                DBInstance? blueInstance = AuroraTestUtils.GetRdsInstanceInfoByArn(blueGreenDeployment.Source);
                if (blueInstance == null)
                {
                    throw new RuntimeException($"Blue instance not found. bgd: {bgId}");
                }

                DBInstance? greenInstance = AuroraTestUtils.GetRdsInstanceInfoByArn(blueGreenDeployment.Target);
                if (greenInstance == null)
                {
                    throw new RuntimeException($"Green instance not found. bgd: {bgId}");
                }

                return [blueInstance.Endpoint.Address, greenInstance.Endpoint.Address];
            case DatabaseEngineDeployment.AURORA:
                HashSet<string> endpoints = [];
                DBCluster? blueCluster = AuroraTestUtils.GetDBClusterByArn(blueGreenDeployment.Source);
                if (blueCluster == null)
                {
                    throw new RuntimeException("Blue Cluster not found");
                }

                if (IncludeClusterEndpoints)
                {
                    endpoints.Add(TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpoint);
                }

                if (IncludeWriterAndReaderOnly)
                {
                    endpoints.Add(TestEnvironment.Env.Info.DatabaseInfo.Instances.First().Host);
                    if (TestEnvironment.Env.Info.DatabaseInfo.Instances.Count > 1)
                    {
                        endpoints.Add(TestEnvironment.Env.Info.DatabaseInfo.Instances[1].Host);
                    }
                }
                else
                {
                    endpoints.AddRange(TestEnvironment.Env.Info.DatabaseInfo.Instances.Select(instanceInfo => instanceInfo.Host));
                }

                DBCluster? greenCluster = AuroraTestUtils.GetDBClusterByArn(blueGreenDeployment.Target);
                if (greenCluster == null)
                {
                    throw new RuntimeException("Green Cluster not found");
                }

                if (IncludeClusterEndpoints)
                {
                    endpoints.Add(greenCluster.Endpoint);
                }

                List<string> instanceIds = AuroraTestUtils.GetAuroraInstanceIds(
                    TestEnvironment.Env.Info.Request.Engine,
                    TestEnvironment.Env.Info.Request.Deployment,
                    TestEnvironment.Env.Info.DatabaseInfo.DefaultDbName,
                    TestEnvironment.Env.Info.DatabaseInfo.Username,
                    TestEnvironment.Env.Info.DatabaseInfo.Password,
                    greenCluster.Endpoint,
                    TestEnvironment.Env.Info.DatabaseInfo.Instances[0].Port);
                if (instanceIds.Count == 0)
                {
                    throw new InvalidOperationException("Can't find green cluster instances.");
                }

                string instancePattern = RdsUtils.GetRdsInstanceHostPattern(greenCluster.Endpoint);
                if (IncludeWriterAndReaderOnly)
                {
                    endpoints.Add(instancePattern.Replace("?", instanceIds.First()));
                    if (instanceIds.Count > 1)
                    {
                        endpoints.Add(instancePattern.Replace("?", instanceIds[1]));
                    }
                }
                else
                {
                    endpoints.AddRange(instanceIds.Select(instanceId => instancePattern.Replace("?", instanceId)));
                }

                return endpoints.ToList();
            default:
                throw new InvalidOperationException("Unsupported " + TestEnvironment.Env.Info.Request.Deployment);
        }
    }

    private long GetPercentile(List<long>? input, double percentile)
    {
        if (input == null || input.Count == 0)
        {
            return 0;
        }

        var sorted = input.OrderBy(x => x).ToList();
        int rank = percentile == 0 ? 1 : (int)Math.Ceiling(percentile / 100.0 * input.Count);
        return sorted[rank - 1];
    }

    private static string GetWrapperConnectionPlugins()
    {
        return TestEnvironment.Env.Info.Request.Features.Contains(TestEnvironmentFeatures.IAM)
            ? "bg,iam"
            : "bg";
    }

    private static string GetDefaultConnectionPlugins()
    {
        return TestEnvironment.Env.Info.Request.Features.Contains(TestEnvironmentFeatures.IAM)
            ? "iam"
            : string.Empty;
    }

    private int GetPort()
    {
        return TestEnvironment.Env.Info.Request.Engine switch
        {
            DatabaseEngine.MYSQL => 3306,
            DatabaseEngine.PG => 5432,
            _ => throw new NotSupportedException(TestEnvironment.Env.Info.Request.Engine.ToString()),
        };
    }

    private void PrintMetrics()
    {
        long bgTriggerTime = this.GetBgTriggerTime();

        var sortedEntries = this.results
            .OrderBy(x => RdsUtils.IsGreenInstance(x.Key + ".") ? 1 : 0)
            .ThenBy(x => RdsUtils.RemoveGreenInstancePrefix(x.Key).ToLower())
            .ToList();

        var rows = new List<string[]>();

        if (sortedEntries.Count == 0)
        {
            rows.Add(["No entries"]);
        }

        foreach ((string key, BlueGreenResults value) in sortedEntries)
        {
            long startTime = value.StartTime!.Get();
            long threadsSyncTime = value.ThreadsSyncTime!.Get();

            rows.Add([
                key,
                startTime.ToString(),
                threadsSyncTime.ToString(),
                this.GetFormattedNanoTime(value.DirectBlueLostConnectionTime!.Get()),
                this.GetFormattedNanoTime(value.WrapperGreenLostConnectionTime!.Get()),
                this.GetFormattedNanoTime(value.DnsBlueChangedTime!.Get()),
                this.GetFormattedNanoTime(value.DnsGreenRemovedTime!.Get()),
                this.GetFormattedNanoTime(value.GreenNodeChangeNameTime!.Get())
            ]);
        }

        var headers = new[]
        {
            "Instance/endpoint",
            "startTime",
            "threadsSync",
            "direct Blue conn dropped (SELECT 1)",
            "wrapper Green conn dropped (SELECT 1)",
            "Blue DNS updated",
            "Green DNS removed",
            "Green node certificate change",
        };
        Logger.LogTrace("\n{Table}", this.RenderTable(rows, headers, true));

        foreach ((string key, BlueGreenResults value) in sortedEntries)
        {
            if (value.BlueStatusTime.IsEmpty && value.GreenStatusTime.IsEmpty)
            {
                continue;
            }

            this.PrintNodeStatusTimes(key, value);
        }

        foreach ((string key, BlueGreenResults value) in sortedEntries)
        {
            if (value.BlueWrapperConnectTimes.IsEmpty)
            {
                continue;
            }

            this.PrintDurationTimes(key, "Wrapper connection time (ms) to Blue", value.BlueWrapperConnectTimes);
        }

        foreach ((string key, BlueGreenResults value) in sortedEntries)
        {
            if (value.GreenDirectIamIpWithGreenNodeConnectTimes.IsEmpty)
            {
                continue;
            }

            this.PrintDurationTimes(key, "Wrapper IAM (green token) connection time (ms) to Green", value.GreenDirectIamIpWithGreenNodeConnectTimes);
        }

        foreach ((string key, BlueGreenResults value) in sortedEntries)
        {
            if (value.BlueWrapperPreSwitchoverExecuteTimes.IsEmpty)
            {
                continue;
            }

            this.PrintDurationTimes(key, "Wrapper execution time (ms) to Blue", value.BlueWrapperPreSwitchoverExecuteTimes);
        }

        foreach ((string key, BlueGreenResults value) in sortedEntries)
        {
            if (value.GreenWrapperExecuteTimes.IsEmpty)
            {
                continue;
            }

            this.PrintDurationTimes(key, "Wrapper execution time (ms) to Green", value.GreenWrapperExecuteTimes);
        }

        foreach ((string key, BlueGreenResults value) in sortedEntries)
        {
            if (value.HostVerificationResults.IsEmpty)
            {
                continue;
            }

            this.PrintHostVerificationResults(key, value.HostVerificationResults, bgTriggerTime);
        }
    }

    private void PrintHostVerificationResults(
        string node, ConcurrentQueue<HostVerificationResult> currentResults, long timeZeroNano)
    {
        var resultsList = currentResults.ToList();
        var successful = resultsList.Where(r => r.Error == null).ToList();

        long totalAttempts = resultsList.Count;
        long totalSuccessfulVerifications = successful.Count;
        long totalUnsuccessfulVerifications = resultsList.Count(r => r.Error != null);
        long totalConnectionsToBlue = successful.Count(r => r.ConnectedToBlue);
        long totalConnectionsToGreen = successful.Count(r => !r.ConnectedToBlue);

        long switchoverInProgressTime = this.GetSwitchoverInProgressTime(timeZeroNano);
        var blueAfterSwitchover = resultsList
            .Where(r => this.GetTimeOffsetMs(r.Timestamp, timeZeroNano) > switchoverInProgressTime && r.ConnectedToBlue)
            .ToList();

        var rows = new List<string[]>
        {
            new[] { "Total verification attempts", totalAttempts.ToString() },
            new[] { "Total successful connection and verification attempts", totalSuccessfulVerifications.ToString() },
            new[] { "Total unsuccessful/dropped verification attempts (expected during switchover)", totalUnsuccessfulVerifications.ToString() },
            new[] { "Total successful connections to blue", totalConnectionsToBlue.ToString() },
            new[] { "Total successful connections to green", totalConnectionsToGreen.ToString() },
            new[] { "Total connections to old blue after switchover was in progress (ERROR if not 0)", blueAfterSwitchover.Count.ToString() },
        };

        if (blueAfterSwitchover.Count > 0)
        {
            rows.Add(["Time (ms)", "Connection to blue after switchover"]);
            rows.AddRange(blueAfterSwitchover.Select(r => new[] { (r.Timestamp - timeZeroNano).ToString(), $"{r.ConnectedHost} (original: {r.OriginalBlueIp})" }));
        }

        var headers = new[] { "Metric", "Value" };
        Logger.LogTrace("\n{Node}: Host Verification Results\n{Table}", node, this.RenderTable(rows, headers, false));
    }

    private string GetFormattedNanoTime(long timeNano)
    {
        long value = Interlocked.Read(ref timeNano);
        return value == 0 ? "-" : $"{value} ms";
    }

    private void PrintNodeStatusTimes(string node, BlueGreenResults currentResults)
    {
        var statusMap = new Dictionary<string, long>();
        foreach (var kvp in currentResults.BlueStatusTime)
        {
            statusMap[kvp.Key] = kvp.Value;
        }

        foreach (var kvp in currentResults.GreenStatusTime)
        {
            statusMap[kvp.Key] = kvp.Value;
        }

        var sortedStatusNames = statusMap.OrderBy(kvp => kvp.Value).Select(kvp => kvp.Key).ToList();

        var rows = new List<string[]>();
        foreach (var status in sortedStatusNames)
        {
            string sourceTime = currentResults.BlueStatusTime.TryGetValue(status, out long blueTime) ? $"{blueTime} ms" : string.Empty;
            string targetTime = currentResults.GreenStatusTime.TryGetValue(status, out long greenTime) ? $"{greenTime} ms" : string.Empty;
            rows.Add([status, sourceTime, targetTime]);
        }

        var headers = new[] { "Status", "SOURCE", "TARGET" };
        Logger.LogTrace("\n{Node}:\n{Table}", node, this.RenderTable(rows, headers, true));
    }

    private void PrintDurationTimes(
        string node,
        string title,
        ConcurrentQueue<TimeHolder> times)
    {
        var timesList = times.ToList();
        if (timesList.Count == 0)
        {
            return;
        }

        var rows = new List<string[]>();

        long p99Nano = this.GetPercentile(timesList.Select(x => x.ElapsedTime).ToList(), 99.0);
        long p99 = p99Nano;

        rows.Add(["p99", p99.ToString(), string.Empty]);

        var first = timesList.First();
        rows.Add(this.FormatTimeRow(first));

        rows.AddRange(from timeHolder in timesList where timeHolder.ElapsedTime > p99 select this.FormatTimeRow(timeHolder));

        var last = timesList.Last();
        rows.Add(this.FormatTimeRow(last));

        var headers = new[] { "Connect at (ms)", "Connect time/duration (ms)", "Error" };
        Logger.LogTrace("\n{Node}: {Title}\n{Table}", node, title, this.RenderTable(rows, headers, false));
    }

    private string[] FormatTimeRow(TimeHolder t)
    {
        return
        [
            t.StartTime.ToString(),
            t.ElapsedTime.ToString(),
            t.ErrorMessage == null ? string.Empty : t.ErrorMessage[..Math.Min(t.ErrorMessage.Length, 100)].Replace("\n", " ") + "..."
        ];
    }

    private string RenderTable(List<string[]> rows, string[] headers, bool leftAlignForColumn0)
    {
        var allRows = new List<string[]> { headers }.Concat(rows).ToList();
        var colWidths = Enumerable.Range(0, headers.Length)
            .Select(c => allRows.Max(r => c < r.Length ? r[c].Length : 0) + 4)
            .ToArray();

        var sb = new StringBuilder();
        var separator = "+" + string.Join("+", colWidths.Select(w => new string('-', w))) + "+";

        sb.AppendLine(separator);
        foreach (var row in allRows)
        {
            sb.Append('|');
            for (int c = 0; c < colWidths.Length; c++)
            {
                var cell = c < row.Length ? row[c] : string.Empty;
                bool leftAlign = leftAlignForColumn0 && c == 0 && row != headers;
                sb.Append(leftAlign
                    ? $"  {cell.PadRight(colWidths[c] - 2)}"
                    : cell.PadLeft((colWidths[c] + cell.Length) / 2).PadRight(colWidths[c]));
                sb.Append('|');
            }

            sb.AppendLine();
            if (row == headers)
            {
                sb.AppendLine(separator);
            }
        }

        sb.AppendLine(separator);
        return sb.ToString();
    }

    private void LogUnhandledExceptions()
    {
        foreach (var exception in this.unhandledExceptions)
        {
            Logger.LogTrace(exception, "Unhandled exception");
        }
    }

    private void AssertTest()
    {
        this.AssertNoRollback();
        this.AssertSwitchoverCompleted();
        this.AssertWrapperBehavior();
    }

    private void AssertNoRollback()
    {
        Assert.False(
            this.rollbackDetected.Get(),
            $"Blue/Green Deployment rollback detected: {this.rollbackDetails}");

        foreach ((string instanceId, BlueGreenResults instanceResults) in this.results)
        {
            foreach (var status in instanceResults.BlueStatusTime.Keys)
            {
                Assert.False(
                    status.Contains("rollback", StringComparison.OrdinalIgnoreCase),
                    $"Blue/Green Deployment rollback detected in blue status for instance '{instanceId}': {status}");
            }

            foreach (var status in instanceResults.GreenStatusTime.Keys)
            {
                Assert.False(
                    status.Contains("rollback", StringComparison.OrdinalIgnoreCase),
                    $"Blue/Green Deployment rollback detected in green status for instance '{instanceId}': {status}");
            }
        }
    }

    private void AssertSwitchoverCompleted()
    {
        long switchoverCompleteTimeFromStatusTable = this.GetSwitchoverCompleteTimeFromStatusTable();
        Assert.NotEqual(0L, switchoverCompleteTimeFromStatusTable);

        foreach (var (instanceId, result) in this.results)
        {
            if (RdsUtils.IsGreenInstance(instanceId))
            {
                long greenNodeChangeTime = result.GreenNodeChangeNameTime!.Get();
                Assert.NotEqual(0L, greenNodeChangeTime);
            }
        }
    }

    private void AssertWrapperBehavior()
    {
        long bgTriggerTime = this.GetBgTriggerTime();
        long switchoverCompleteTime = this.GetSwitchoverCompleteTime();

        Logger.LogInformation("bgTriggerTime (nanos): {BgTriggerTime} ms", bgTriggerTime);
        Logger.LogInformation("switchoverCompleteTime (ms offset from bgTriggerTime): {SwitchoverCompleteTime} ms", switchoverCompleteTime);

        var connectTimes = this.results.Values.SelectMany(r => r.BlueWrapperConnectTimes).ToList();
        var postSwitchoverExecuteTimes = this.results.Values.SelectMany(r => r.BlueWrapperPostSwitchoverExecuteTimes).ToList();

        long successfulConnections = this.CountSuccessfulOperationsAfterSwitchover(connectTimes, bgTriggerTime, switchoverCompleteTime);
        long successfulExecutions = this.CountSuccessfulOperations(postSwitchoverExecuteTimes);
        long unsuccessfulConnections = this.CountUnsuccessfulOperationsAfterSwitchover(connectTimes, bgTriggerTime, switchoverCompleteTime);
        long unsuccessfulExecutions = this.CountUnsuccessfulOperations(postSwitchoverExecuteTimes);

        Logger.LogTrace("Successful wrapper connections after switchover: {Count}", successfulConnections);
        Logger.LogTrace("Successful wrapper executions after switchover: {Count}", successfulExecutions);
        Logger.LogTrace("Unsuccessful wrapper connections after switchover: {Count}", unsuccessfulConnections);
        Logger.LogTrace("Unsuccessful wrapper executions after switchover: {Count}", unsuccessfulExecutions);

        if (unsuccessfulConnections > 0)
        {
            this.LogUnsuccessfulOperationsAfterSwitchover(connectTimes, bgTriggerTime, switchoverCompleteTime, "connection");
        }

        if (unsuccessfulExecutions > 0)
        {
            this.LogUnsuccessfulOperations(postSwitchoverExecuteTimes, "execution");
        }

        Assert.Equal(0L, unsuccessfulConnections);
        Assert.Equal(0L, unsuccessfulExecutions);
        Assert.True(successfulConnections > 0,
            $"Expected at least one successful wrapper connection after switchover, but found {successfulConnections}.");
        Assert.True(successfulExecutions > 0,
            $"Expected at least one successful wrapper execution after switchover, but found {successfulExecutions}.");

        this.AssertNoConnectionsToOldBlueCluster(bgTriggerTime);
    }

    private void AssertNoConnectionsToOldBlueCluster(long bgTriggerTime)
    {
        long switchoverInitiatedTime = this.GetSwitchoverInitiatedTime(bgTriggerTime);
        long switchoverInProgressTime = this.GetSwitchoverInProgressTime(bgTriggerTime);

        Logger.LogInformation(
            "Host verification timing - Switchover INITIATED (earliest): {InitiatedTime} ms, IN_PROGRESS (latest): {InProgressTime} ms",
            switchoverInitiatedTime,
            switchoverInProgressTime);

        Assert.NotEqual(0L, switchoverInitiatedTime);
        Assert.NotEqual(0L, switchoverInProgressTime);

        // Before switchover initiated: all connections should go to blue
        var beforeSwitchover = this.results.Values
            .SelectMany(r => r.HostVerificationResults)
            .Where(r => this.GetTimeOffsetMs(r.Timestamp, bgTriggerTime) < switchoverInitiatedTime)
            .Where(r => r.Error == null)
            .ToList();

        long connectionsBeforeSwitchover = beforeSwitchover.Count;
        long connectionsToBlueBeforeSwitchover = beforeSwitchover.Count(r => r.ConnectedToBlue);
        long connectionsToGreenBeforeSwitchover = beforeSwitchover.Count(r => !r.ConnectedToBlue);

        Logger.LogInformation(
            "Before SWITCHOVER_INITIATED ({InitiatedTime} ms): {Total} total connections, {Blue} to blue, {Green} to green",
            switchoverInitiatedTime,
            connectionsBeforeSwitchover,
            connectionsToBlueBeforeSwitchover,
            connectionsToGreenBeforeSwitchover);

        Assert.Equal(connectionsBeforeSwitchover, connectionsToBlueBeforeSwitchover);
        Assert.Equal(0L, connectionsToGreenBeforeSwitchover);

        // After switchover in progress: no connections should go to old blue
        var afterSwitchover = this.results.Values
            .SelectMany(r => r.HostVerificationResults)
            .Where(r => this.GetTimeOffsetMs(r.Timestamp, bgTriggerTime) > switchoverInProgressTime)
            .ToList();

        long connectionsToBlueAfterSwitchoverStart = afterSwitchover.Count(r => r.ConnectedToBlue);
        long connectionsToGreenAfterSwitchoverStart = afterSwitchover.Count(r => r.Error == null && !r.ConnectedToBlue);
        long totalVerificationsAfterSwitchoverStart = afterSwitchover.Count(r => r.Error == null);

        Logger.LogInformation(
            "After switchover IN_PROGRESS ({InProgressTime} ms): {Total} total connections, {Old} to old host, {New} to new host",
            switchoverInProgressTime,
            totalVerificationsAfterSwitchoverStart,
            connectionsToBlueAfterSwitchoverStart,
            connectionsToGreenAfterSwitchoverStart);

        if (connectionsToBlueAfterSwitchoverStart > 0)
        {
            foreach (var r in afterSwitchover.Where(r => r.ConnectedToBlue))
            {
                Logger.LogWarning(
                    "Connected to old blue cluster at offset {Offset} ms (after IN_PROGRESS at {InProgress} ms): connected={Host}, originalBlue={Blue}",
                    this.GetTimeOffsetMs(r.Timestamp, bgTriggerTime),
                    switchoverInProgressTime,
                    r.ConnectedHost,
                    r.OriginalBlueIp);
            }
        }

        Assert.Equal(0L, connectionsToBlueAfterSwitchoverStart);
        Assert.True(totalVerificationsAfterSwitchoverStart > 0,
            "Expected at least one successful host verification after SWITCHOVER_IN_PROGRESS.");
    }

    private long GetSwitchoverInitiatedTime(long bgTriggerTime)
    {
        var times = this.results.Values
            .SelectMany(r => new[] { r.BlueStatusTime, r.GreenStatusTime })
            .Select(dict => dict.GetValueOrDefault("SWITCHOVER_INITIATED", 0L))
            .Where(t => t > 0)
            .Select(t => this.GetTimeOffsetMs(t, bgTriggerTime))
            .Where(t => t > 0)
            .ToList();

        return times.Count > 0 ? times.Min() : 0L;
    }

    private long GetSwitchoverInProgressTime(long bgTriggerTime)
    {
        var times = this.results.Values
            .SelectMany(r => new[] { r.BlueStatusTime, r.GreenStatusTime })
            .Select(dict => dict.GetValueOrDefault("SWITCHOVER_IN_PROGRESS", 0L))
            .Where(t => t > 0)
            .Select(t => this.GetTimeOffsetMs(t, bgTriggerTime))
            .ToList();

        return times.Count > 0 ? times.Max() + 1000 : 0L;
    }

    private long GetBgTriggerTime()
    {
        return this.results.Values.Count == 0 ? throw new InvalidOperationException("Can't get bgTriggerTime") : this.results.Values.First().BgTriggerTime!.Get();
    }

    private long GetSwitchoverCompleteTimeFromStatusTable()
    {
        long bgTriggerTime = this.GetBgTriggerTime();
        var times = this.results.Values
            .Where(x => !x.GreenStatusTime.IsEmpty)
            .Select(x => this.GetTimeOffsetMs(x.GreenStatusTime.GetValueOrDefault("SWITCHOVER_COMPLETED", 0L), bgTriggerTime))
            .ToList();
        long time = times.Count > 0 ? times.Max() : 0L;
        Logger.LogTrace("switchoverCompleteTimeFromStatusTable: {Time} ms", time);
        return time;
    }

    private long GetMaxGreenNodeChangeTime()
    {
        long bgTriggerTime = this.GetBgTriggerTime();
        var times = this.results.Values
            .Select(r => this.GetTimeOffsetMs(r.GreenNodeChangeNameTime!.Get(), bgTriggerTime))
            .ToList();
        long time = times.Count > 0 ? times.Max() : 0L;
        Logger.LogTrace("maxGreenNodeChangeTime: {Time} ms", time);
        return time;
    }

    private long GetSwitchoverCompleteTime()
    {
        long time = Math.Max(this.GetMaxGreenNodeChangeTime(), this.GetSwitchoverCompleteTimeFromStatusTable());
        Logger.LogTrace("switchoverCompleteTime: {Time} ms", time);
        return time;
    }

    private long GetTimeOffsetMs(long nanoTime, long bgTriggerTime)
    {
        return nanoTime == 0 ? 0 : (nanoTime - bgTriggerTime);
    }

    private long CountSuccessfulOperationsAfterSwitchover(
        IEnumerable<TimeHolder> times,
        long bgTriggerTime,
        long switchoverCompleteTime)
    {
        return times.Count(t => this.GetTimeOffsetMs(t.StartTime, bgTriggerTime) > switchoverCompleteTime && t.ErrorMessage == null);
    }

    private long CountUnsuccessfulOperationsAfterSwitchover(
        IEnumerable<TimeHolder> times,
        long bgTriggerTime,
        long switchoverCompleteTime)
    {
        return times.Count(t => this.GetTimeOffsetMs(t.StartTime, bgTriggerTime) > switchoverCompleteTime && t.ErrorMessage != null);
    }

    private long CountSuccessfulOperations(IEnumerable<TimeHolder> times)
    {
        return times.Count(t => t.ErrorMessage == null);
    }

    private long CountUnsuccessfulOperations(IEnumerable<TimeHolder> times)
    {
        return times.Count(t => t.ErrorMessage != null);
    }

    private void LogUnsuccessfulOperationsAfterSwitchover(
        IEnumerable<TimeHolder> times,
        long bgTriggerTime,
        long switchoverCompleteTime,
        string operationType)
    {
        foreach (var t in times.Where(t => this.GetTimeOffsetMs(t.StartTime, bgTriggerTime) > switchoverCompleteTime && t.ErrorMessage != null))
        {
            Logger.LogInformation(
                "Unsuccessful {OperationType} at offset {Offset} ms (after switchover at {SwitchoverTime} ms): {Error}",
                operationType,
                this.GetTimeOffsetMs(t.StartTime, bgTriggerTime),
                switchoverCompleteTime,
                t.ErrorMessage);
        }
    }

    private void LogUnsuccessfulOperations(IEnumerable<TimeHolder> times, string operationType)
    {
        foreach (var t in times.Where(t => t.ErrorMessage != null))
        {
            Logger.LogInformation("Unsuccessful {OperationType}: {Error}", operationType, t.ErrorMessage);
        }
    }

    private static bool IsConnectionClosed(DbConnection connection)
    {
        return connection.State is ConnectionState.Closed or ConnectionState.Broken;
    }
}

public class TimeHolder
{
    public long StartTime { get; set; }

    public long ElapsedTime { get; set; }

    public string? ErrorMessage { get; set; }

    public long HoldTimeMs { get; set; }

    public TimeHolder(long startTime, long elapsedTime)
    {
        this.StartTime = startTime;
        this.ElapsedTime = elapsedTime;
    }

    public TimeHolder(long startTime, long elapsedTime, long holdTimeMs)
        : this(startTime, elapsedTime)
    {
        this.HoldTimeMs = holdTimeMs;
    }

    public TimeHolder(long startTime, long elapsedTime, string error)
        : this(startTime, elapsedTime)
    {
        this.ErrorMessage = error;
    }

    public TimeHolder(long startTime, long elapsedTime, long holdTimeMs, string error)
        : this(startTime, elapsedTime)
    {
        this.HoldTimeMs = holdTimeMs;
        this.ErrorMessage = error;
    }
}

public class HostVerificationResult
{
    public long Timestamp { get; }

    public string? ConnectedHost { get; }

    public string OriginalBlueIp { get; }

    public bool ConnectedToBlue { get; }

    public string? Error { get; }

    private HostVerificationResult(long timestamp, string? connectedHost, string originalBlueIp, string? error)
    {
        this.Timestamp = timestamp;
        this.ConnectedHost = connectedHost;
        this.OriginalBlueIp = originalBlueIp;
        this.ConnectedToBlue = connectedHost != null && connectedHost.Equals(originalBlueIp);
        this.Error = error;
    }

    public static HostVerificationResult Success(long timestamp, string connectedHost, string originalBlueIp)
    {
        return new HostVerificationResult(timestamp, connectedHost, originalBlueIp, null);
    }

    public static HostVerificationResult Failure(long timestamp, string originalBlueIp, string error)
    {
        return new HostVerificationResult(timestamp, null, originalBlueIp, error);
    }
}

public class BlueGreenResults
{
    public readonly ConcurrentDictionary<string, long> BlueStatusTime = new();
    public readonly ConcurrentDictionary<string, long> GreenStatusTime = new();
    public readonly ConcurrentQueue<TimeHolder> BlueWrapperConnectTimes = new();
    public readonly ConcurrentQueue<TimeHolder> BlueWrapperPreSwitchoverExecuteTimes = new();
    public readonly ConcurrentQueue<TimeHolder> BlueWrapperPostSwitchoverExecuteTimes = new();
    public readonly ConcurrentQueue<TimeHolder> GreenWrapperExecuteTimes = new();
    public readonly ConcurrentQueue<TimeHolder> GreenDirectIamIpWithBlueNodeConnectTimes = new();
    public readonly ConcurrentQueue<TimeHolder> GreenDirectIamIpWithGreenNodeConnectTimes = new();
    public readonly ConcurrentQueue<HostVerificationResult> HostVerificationResults = new();
    public AtomicLong? StartTime = new();
    public AtomicLong? ThreadsSyncTime = new();
    public AtomicLong? BgTriggerTime = new();
    public AtomicLong? DirectBlueLostConnectionTime = new();
    public AtomicLong? WrapperGreenLostConnectionTime = new();
    public AtomicLong? DnsBlueChangedTime = new();
    public string? DnsBlueError;
    public AtomicLong? DnsGreenRemovedTime = new();
    public AtomicLong? GreenNodeChangeNameTime = new();
}
