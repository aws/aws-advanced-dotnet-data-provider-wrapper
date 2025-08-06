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
using Amazon;
using Amazon.RDS;
using Amazon.RDS.Model;
using Amazon.Runtime;
using Amazon.Runtime.Credentials;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Tests.Container.Utils;

public class AuroraTestUtils
{
    private readonly AmazonRDSClient rdsClient;

    public AuroraTestUtils(string region, string? endpoint) : this(
            region: GetRegionInternal(region),
            rdsEndpoint: endpoint,
            DefaultAWSCredentialsIdentityResolver.GetCredentials())
    {
    }

    public AuroraTestUtils(string region, string? rdsEndpoint, string awsAccessKeyId, string awsSecretAccessKey, string awsSessionToken)
        : this(
        region: GetRegionInternal(region),
        rdsEndpoint: rdsEndpoint,
        credentials: new SessionAWSCredentials(awsAccessKeyId, awsSecretAccessKey, awsSessionToken))
    {
    }

    public AuroraTestUtils(RegionEndpoint region, string? rdsEndpoint, AWSCredentials credentials)
    {
        var rdsConfig = new AmazonRDSConfig
        {
            RegionEndpoint = region,
        };

        if (!string.IsNullOrEmpty(rdsEndpoint))
        {
            rdsConfig.ServiceURL = rdsEndpoint;
        }

        this.rdsClient = new AmazonRDSClient(credentials, rdsConfig);
    }

    public static AuroraTestUtils GetUtility()
    {
        return GetUtility(null);
    }

    public static AuroraTestUtils GetUtility(TestEnvironmentInfo? info)
    {
        info ??= TestEnvironment.Env.Info;

        return new AuroraTestUtils(info.Region!, info.RdsEndpoint);
    }

    private static RegionEndpoint GetRegionInternal(string rdsRegion)
    {
        RegionEndpoint? region = RegionEndpoint.EnumerableAllRegions.FirstOrDefault(r => string.Equals(r.SystemName, rdsRegion, StringComparison.OrdinalIgnoreCase));

        return region ?? throw new ArgumentException($"Unknown AWS region '{rdsRegion}'.");
    }

    public static bool IsSuccessfulResponse(AmazonWebServiceResponse response)
    {
        int statusCode = (int)response.HttpStatusCode;
        return statusCode >= 200 && statusCode < 300;
    }

    public async Task WaitUntilClusterHasRightStateAsync(string clusterId)
    {
        await this.WaitUntilClusterHasRightStateAsync(clusterId, "available");
    }

    public async Task WaitUntilClusterHasRightStateAsync(string clusterId, params string[] allowedStatuses)
    {
        var allowedStatusSet = new HashSet<string>(allowedStatuses.Select(s => s.ToLower()));
        var timeout = TimeSpan.FromMinutes(15);
        var stopwatch = Stopwatch.StartNew();

        string status = (await this.GetDBClusterAsync(clusterId))!.Status;
        Console.WriteLine($"Cluster status: {status}, waiting for: {string.Join(", ", allowedStatuses)}");

        while (!allowedStatusSet.Contains(status!.ToLower()) && stopwatch.Elapsed < timeout)
        {
            await Task.Delay(1000);
            var tmpStatus = (await this.GetDBClusterAsync(clusterId))?.Status!;
            if (!string.Equals(tmpStatus, status, StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"Cluster status (waiting): {tmpStatus}");
            }

            status = tmpStatus;
        }

        Console.WriteLine($"Cluster status (after wait): {status}");
    }

    public async Task WaitUntilInstanceHasRightStateAsync(string instanceId, params string[] allowedStatuses)
    {
        string status = (await this.GetDBInstanceAsync(instanceId))!.DBInstanceStatus;
        Console.WriteLine($"Instance {instanceId} status: {status}, waiting for status: {string.Join(", ", allowedStatuses)}");

        var allowedStatusSet = new HashSet<string>(allowedStatuses.Select(s => s.ToLower()));
        var timeout = TimeSpan.FromMinutes(15);
        var stopwatch = Stopwatch.StartNew();

        while (!allowedStatusSet.Contains(status.ToLower()) && stopwatch.Elapsed < timeout)
        {
            await Task.Delay(1000);
            string tmpStatus = (await this.GetDBInstanceAsync(instanceId))!.DBInstanceStatus;

            if (!tmpStatus.Equals(status, StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"Instance {instanceId} status (waiting): {tmpStatus}");
            }

            status = tmpStatus;
        }

        Console.WriteLine($"Instance {instanceId} status (after wait): {status}");
    }

    public async Task MakeSureInstancesUpAsync(TimeSpan timeout)
    {
        var envInfo = TestEnvironment.Env.Info;
        List<TestInstanceInfo> instances = [.. envInfo.DatabaseInfo.Instances, .. envInfo.ProxyDatabaseInfo!.Instances];
        await this.MakeSureInstancesUpAsync(instances, timeout);
    }

    public async Task MakeSureInstancesUpAsync(List<TestInstanceInfo> instances, TimeSpan timeout)
    {
        var remainingInstances = new ConcurrentDictionary<string, bool>();
        var dbName = TestEnvironment.Env.Info.DatabaseInfo.DefaultDbName;
        var username = TestEnvironment.Env.Info.DatabaseInfo.Username;
        var password = TestEnvironment.Env.Info.DatabaseInfo.Password;
        var engine = TestEnvironment.Env.Info.Request.Engine;

        foreach (var instance in instances)
        {
            remainingInstances[instance.Host] = true;
        }

        var tasks = new List<Task>();
        var cts = new CancellationTokenSource(timeout);

        foreach (var instance in instances)
        {
            tasks.Add(Task.Run(async () =>
            {
                var host = instance.Host;
                var port = instance.Port;
                var url = ConnectionStringHelper.GetUrl(engine, host, port, username, password, dbName);

                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        using var connection = DriverHelper.CreateUnopenedConnection(engine, url);
                        await connection.OpenAsync(cts.Token);
                        if (connection.State == ConnectionState.Open)
                        {
                            Console.WriteLine($"Host {host} is up.");
                            if (host.Contains(".proxied"))
                            {
                                Console.WriteLine($"Proxied host {host} resolves to IP address {this.HostToIP(host, true)}");
                            }

                            remainingInstances.TryRemove(host, out _);
                            break;
                        }
                    }
                    catch (DbException ex)
                    {
                        Console.WriteLine($"Exception while trying to connect to host {host}: {ex.Message}");
                    }
                    catch (TaskCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Unexpected error on {host}: {ex.Message}");
                        break;
                    }

                    try
                    {
                        await Task.Delay(5000, cts.Token);
                    }
                    catch (TaskCanceledException)
                    {
                        break;
                    }
                }
            },
            cts.Token));
        }

        try
        {
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Timed out while waiting for instances to come up.");
        }

        if (!remainingInstances.IsEmpty)
        {
            throw new Exception("The following instances are still down:\n" + string.Join("\n", remainingInstances.Keys));
        }
    }

    public string? HostToIP(string hostname, bool fail)
    {
        int remainingTries = 5;
        string ipAddress;

        while (remainingTries-- > 0)
        {
            try
            {
                var inet = Dns.GetHostEntry(hostname);
                if (inet.AddressList.Length > 0)
                {
                    ipAddress = inet.AddressList[0].ToString();
                    return ipAddress;
                }
            }
            catch (Exception)
            {
                // Swallow exception, retry
            }

            Thread.Sleep(TimeSpan.FromSeconds(1));
        }

        if (remainingTries <= 0 && fail)
        {
            throw new Exception($"The IP address of host {hostname} could not be determined");
        }

        return null;
    }

    public async Task<DBInstance?> GetDBInstanceAsync(string instanceId)
    {
        int remainingTries = 5;
        for (int i = 0; i < remainingTries; i++)
        {
            try
            {
                var response = await this.rdsClient.DescribeDBInstancesAsync(
                    new DescribeDBInstancesRequest
                    {
                        DBInstanceIdentifier = instanceId,
                    });

                return response.DBInstances.First();
            }
            catch (AmazonServiceException) when (i < 4)
            {
                await Task.Delay(TimeSpan.FromSeconds(30));
            }
        }

        throw new Exception($"Unable to get DB instance info for instance with ID {instanceId}");
    }

    public async Task<bool> IsDBInstanceWriterAsync(string instanceId)
    {
        return await this.IsDBInstanceWriterAsync(TestEnvironment.Env.Info.RdsDbName!, instanceId);
    }

    public async Task<bool> IsDBInstanceWriterAsync(string clusterId, string instanceId)
    {
        var dbClusterMember = await this.GetMatchedDBClusterMemberAsync(clusterId, instanceId);
        if (dbClusterMember.IsClusterWriter == null)
        {
            throw new InvalidOperationException($"DBClusterMember.IsClusterWriter is null for instance {instanceId} in cluster {clusterId}.");
        }

        return dbClusterMember.IsClusterWriter.Value;
    }

    public async Task<DBClusterMember> GetMatchedDBClusterMemberAsync(string clusterId, string instanceId)
    {
        var members = await this.GetDBClusterMemberListAsync(clusterId);
        var matchedMember = members.FirstOrDefault(m => m.DBInstanceIdentifier == instanceId);

        if (matchedMember == null)
        {
            throw new InvalidOperationException($"Cannot find cluster member whose DB instance identifier is {instanceId}");
        }

        return matchedMember;
    }

    public async Task<List<DBClusterMember>> GetDBClusterMemberListAsync(string clusterId)
    {
        var cluster = await this.GetDBClusterAsync(clusterId);
        if (cluster == null || cluster.DBClusterMembers == null)
        {
            throw new InvalidOperationException($"Unable to retrieve DB cluster members for cluster with ID {clusterId}");
        }

        return cluster.DBClusterMembers;
    }

    public async Task<DBCluster?> GetDBClusterAsync(string clusterId)
    {
        DescribeDBClustersResponse? response = null;
        int remainingTries = 5;

        while (remainingTries-- > 0)
        {
            try
            {
                response = await this.rdsClient.DescribeDBClustersAsync(
                    new DescribeDBClustersRequest
                    {
                        DBClusterIdentifier = clusterId,
                    });

                break;
            }
            catch (DBClusterNotFoundException)
            {
                return null;
            }
            catch (AmazonServiceException)
            {
                if (remainingTries == 0)
                {
                    throw;
                }
            }
        }

        if (response == null || response.DBClusters.Count == 0)
        {
            throw new InvalidOperationException($"Unable to get DB cluster info for cluster with ID {clusterId}");
        }

        return response.DBClusters.First();
    }

    public List<string> GetAuroraInstanceIds()
    {
        var databaseEngine = TestEnvironment.Env.Info.Request.Engine;
        var deployment = TestEnvironment.Env.Info.Request.Deployment;
        var dbName = TestEnvironment.Env.Info.DatabaseInfo.DefaultDbName;
        var username = TestEnvironment.Env.Info.DatabaseInfo.Username;
        var password = TestEnvironment.Env.Info.DatabaseInfo.Password;
        var host = TestEnvironment.Env.Info.DatabaseInfo.Instances[0].Host;
        var port = TestEnvironment.Env.Info.DatabaseInfo.Instances[0].Port;
        var connectionUrl = ConnectionStringHelper.GetUrl(databaseEngine, host, port, username, password, dbName);
        string retrieveTopologySql = deployment switch
        {
            DatabaseEngineDeployment.AURORA => databaseEngine switch
            {
                DatabaseEngine.MYSQL => "SELECT SERVER_ID, SESSION_ID FROM information_schema.replica_host_status " +
                                            "ORDER BY IF(SESSION_ID = 'MASTER_SESSION_ID', 0, 1)",
                DatabaseEngine.PG => "SELECT SERVER_ID, SESSION_ID FROM aurora_replica_status() " +
                                            "ORDER BY CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN 0 ELSE 1 END",
                _ => throw new NotSupportedException(databaseEngine.ToString()),
            },
            _ => throw new NotSupportedException(deployment.ToString()),
        };
        var auroraInstances = new List<string>();

        using var connection = DriverHelper.CreateUnopenedConnection(databaseEngine, connectionUrl);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = retrieveTopologySql;
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            auroraInstances.Add(reader.GetString(reader.GetOrdinal("SERVER_ID")));
        }

        return auroraInstances;
    }

    public bool WaitForDnsCondition(string hostToCheck, string targetHostIpOrName, TimeSpan timeout, bool expectEqual, bool fail)
    {
        string? hostIpAddress = this.HostToIP(hostToCheck, false);
        if (hostIpAddress == null)
        {
            var startTime = Stopwatch.StartNew();
            while (hostIpAddress == null && startTime.Elapsed < timeout)
            {
                Thread.Sleep(TimeSpan.FromSeconds(5));
                hostIpAddress = this.HostToIP(hostToCheck, false);
            }

            if (hostIpAddress == null)
            {
                throw new Exception($"Can't get IP address for {hostToCheck}");
            }
        }

        string? expectedHostIpAddress = RdsUtils.IsIp(targetHostIpOrName)
            ? targetHostIpOrName
            : this.HostToIP(targetHostIpOrName, true);

        Console.WriteLine($"Wait for {hostToCheck} (current IP address {hostIpAddress}) resolves to {targetHostIpOrName} (IP address {expectedHostIpAddress})");

        var checkStartTime = Stopwatch.StartNew();
        var stillNotExpected = expectEqual ? expectedHostIpAddress != hostIpAddress : expectedHostIpAddress == hostIpAddress;
        while (stillNotExpected && checkStartTime.Elapsed < timeout)
        {
            Thread.Sleep(TimeSpan.FromSeconds(5));
            hostIpAddress = this.HostToIP(hostToCheck, false);
            Console.WriteLine($"{hostToCheck} resolves to {hostIpAddress}");
        }

        bool result = expectEqual ? expectedHostIpAddress == hostIpAddress : expectedHostIpAddress != hostIpAddress;
        if (fail && !result)
        {
            throw new Exception("DNS resolution did not match expected value.");
        }

        Console.WriteLine("Completed.");
        return result;
    }

    public async Task RebootInstanceAsync(string instanceId)
    {
        int remainingAttempts = 5;

        while (--remainingAttempts > 0)
        {
            try
            {
                var response = await this.rdsClient.RebootDBInstanceAsync(
                    new RebootDBInstanceRequest
                    {
                        DBInstanceIdentifier = instanceId,
                    });

                if (!IsSuccessfulResponse(response))
                {
                    Console.WriteLine($"rebootDBInstance for {instanceId} response: {response.HttpStatusCode}");
                }
                else
                {
                    Console.WriteLine($"rebootDBInstance for {instanceId} request is sent");
                    return;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"rebootDBInstance '{instanceId}' instance request failed: {ex.Message}");
                await Task.Delay(1000);
            }
        }

        throw new InvalidOperationException($"Failed to request an instance {instanceId} reboot.");
    }
}
