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
using System.Globalization;
using System.Net;
using Amazon;
using Amazon.RDS;
using Amazon.RDS.Model;
using Amazon.Runtime;
using Amazon.Runtime.Credentials;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Tests.Container.Utils;

public class AuroraTestUtils
{
    private readonly AmazonRDSClient rdsClient;
    private readonly AmazonSecretsManagerClient secretsManagerClient;

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

        var secretsConfig = new AmazonSecretsManagerConfig
        {
            RegionEndpoint = region,
        };

        this.rdsClient = new AmazonRDSClient(credentials, rdsConfig);
        this.secretsManagerClient = new AmazonSecretsManagerClient(credentials, secretsConfig);
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
        Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Cluster {clusterId} status: {status}, waiting for: {string.Join(", ", allowedStatuses)}");

        while (!allowedStatusSet.Contains(status!.ToLower()) && stopwatch.Elapsed < timeout)
        {
            await Task.Delay(1000);
            var tmpStatus = (await this.GetDBClusterAsync(clusterId))?.Status!;
            if (!string.Equals(tmpStatus, status, StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Cluster {clusterId} status (waiting): {tmpStatus}");
            }

            status = tmpStatus;
        }

        Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Cluster {clusterId} status (after wait): {status}");
    }

    public async Task WaitUntilInstanceHasRightStateAsync(string instanceId, params string[] allowedStatuses)
    {
        string status = (await this.GetDBInstanceAsync(instanceId))!.DBInstanceStatus;
        Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Instance {instanceId} status: {status}, waiting for status: {string.Join(", ", allowedStatuses)}");

        var allowedStatusSet = new HashSet<string>(allowedStatuses.Select(s => s.ToLower()));
        var timeout = TimeSpan.FromMinutes(15);
        var stopwatch = Stopwatch.StartNew();

        while (!allowedStatusSet.Contains(status.ToLower()) && stopwatch.Elapsed < timeout)
        {
            await Task.Delay(1000);
            string tmpStatus = (await this.GetDBInstanceAsync(instanceId))!.DBInstanceStatus;

            if (!tmpStatus.Equals(status, StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Instance {instanceId} status (waiting): {tmpStatus}");
            }

            status = tmpStatus;
        }

        Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Instance {instanceId} status (after wait): {status}");
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
                        await using var connection = DriverHelper.CreateUnopenedConnection(engine, url);
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

                        Console.WriteLine($"Host {host} connection state is {connection.State}.");
                    }
                    catch (DbException ex)
                    {
                        Console.WriteLine($"Exception while trying to connect to host {host}: {ex.Message}");
                    }
                    catch (TaskCanceledException ex)
                    {
                        Console.WriteLine($"Task is cancelled while waiting for {host} to come up: {ex.Message}");
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
                    catch (TaskCanceledException ex)
                    {
                        Console.WriteLine($"Task is cancelled while waiting for {host} to come up: {ex.Message}");
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

        return dbClusterMember.IsClusterWriter.HasValue && dbClusterMember.IsClusterWriter.Value;
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
        var results = new List<DBCluster>();
        DescribeDBClustersRequest request = new()
        {
            DBClusterIdentifier = clusterId,
        };

        while (remainingTries-- > 0)
        {
            try
            {
                // Get the full list if there are multiple pages.
                do
                {
                    response = await this.rdsClient.DescribeDBClustersAsync(request);
                    if (response.DBClusters != null)
                    {
                        results.AddRange(response.DBClusters);
                    }

                    request.Marker = response.Marker;
                }
                while (response.Marker is not null);

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

        if (response == null || results.Count == 0)
        {
            throw new InvalidOperationException($"Unable to get DB cluster info for cluster with ID {clusterId}");
        }

        return results.First();
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

        using var connection = DriverHelper.CreateUnopenedConnection(databaseEngine, connectionUrl);
        connection.Open();

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
            DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER => databaseEngine switch
            {
                DatabaseEngine.MYSQL => "SELECT SUBSTRING_INDEX(endpoint, '.', 1) as SERVER_ID FROM mysql.rds_topology"
                                        + " ORDER BY CASE WHEN id = "
                                        + (this.GetMultiAzMysqlReplicaWriterInstanceId(connection) is { } id ? $"'{id}'" : "@@server_id")
                                        + " THEN 0 ELSE 1 END, SUBSTRING_INDEX(endpoint, '.', 1)",
                DatabaseEngine.PG => "SELECT SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint)) as SERVER_ID"
                                     + " FROM rds_tools.show_topology()"
                                     + " ORDER BY CASE WHEN id ="
                                     + " (SELECT MAX(multi_az_db_cluster_source_dbi_resource_id) FROM"
                                     + " rds_tools.multi_az_db_cluster_source_dbi_resource_id())"
                                     + " THEN 0 ELSE 1 END, endpoint",
                _ => throw new NotSupportedException(databaseEngine.ToString()),
            },
            DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE => databaseEngine switch
            {
                DatabaseEngine.MYSQL => "SELECT SUBSTRING_INDEX(endpoint, '.', 1) as SERVER_ID FROM mysql.rds_topology",
                DatabaseEngine.PG => "SELECT SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint)) as SERVER_ID"
                                     + " FROM rds_tools.show_topology()",
                _ => throw new NotSupportedException(databaseEngine.ToString()),
            },
            _ => throw new NotSupportedException(deployment.ToString()),
        };
        var auroraInstances = new List<string>();

        Console.WriteLine($"Retrieving Aurora instances using SQL: {retrieveTopologySql}");

        using var command = connection.CreateCommand();
        command.CommandText = retrieveTopologySql;
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            auroraInstances.Add(reader.GetString(reader.GetOrdinal("SERVER_ID")));
        }

        return auroraInstances;
    }

    private string? GetMultiAzMysqlReplicaWriterInstanceId(DbConnection connection)
    {
        try
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SHOW REPLICA STATUS";
                using var reader = command.ExecuteReader();

                if (!reader.Read())
                {
                    return null;
                }

                int i = reader.GetOrdinal("Source_Server_id");
                return reader.IsDBNull(i)
                    ? null
                    : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error occurred when getting Source_Server_id: {ex.Message}");
            return null;
        }
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

        Console.WriteLine($"Wait for {hostToCheck} (current IP address {hostIpAddress}) resolves to {(expectEqual ? string.Empty : "anything except ")}{targetHostIpOrName} (IP address {expectedHostIpAddress})");

        var checkStartTime = Stopwatch.StartNew();
        bool StillNotExpected() => expectEqual ? expectedHostIpAddress != hostIpAddress : expectedHostIpAddress == hostIpAddress;
        while (StillNotExpected() && checkStartTime.Elapsed < timeout)
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

    public async Task RebootClusterAsync(string clusterName)
    {
        int remainingAttempts = 5;
        while (--remainingAttempts > 0)
        {
            try
            {
                var request = new RebootDBClusterRequest { DBClusterIdentifier = clusterName, };

                var response = await this.rdsClient.RebootDBClusterAsync(request);

                if (!IsSuccessfulResponse(response))
                {
                    Console.WriteLine($"rebootDBCluster response: {response.HttpStatusCode}");
                }
                else
                {
                    Console.WriteLine("rebootDBCluster request is sent successfully");
                    return;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"rebootDBCluster '{clusterName}' cluster request failed: {ex.Message}");
                await Task.Delay(1000);
            }
        }

        throw new InvalidOperationException($"Failed to request an cluster {clusterName} reboot.");
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

    public string CreateSecrets(string secretsName)
    {
        var request = new CreateSecretRequest
        {
            Name = secretsName,
            SecretString =
                $"{{\"username\":\"{TestEnvironment.Env.Info.DatabaseInfo.Username}\"," +
                $"\"password\":\"{TestEnvironment.Env.Info.DatabaseInfo.Password}\"," +
                $"\"engine\":\"{TestEnvironment.Env.Info.Request.Engine}\"," +
                $"\"dbname\":\"{TestEnvironment.Env.Info.DatabaseInfo.DefaultDbName}\"," +
                $"\"host\":\"{TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpoint}\"," +
                $"\"description\":\"Test secret generated by integration tests.\"}}",
        };

        var response = this.secretsManagerClient.CreateSecretAsync(request).GetAwaiter().GetResult();
        return response.ARN;
    }

    public void DeleteSecrets(string secretsName)
    {
        var request = new DeleteSecretRequest
        {
            SecretId = secretsName,
            ForceDeleteWithoutRecovery = true,
        };
        this.secretsManagerClient.DeleteSecretAsync(request).GetAwaiter().GetResult();
    }

    public async Task<string> GetDBClusterWriterInstanceIdAsync(string clusterId)
    {
        var matchedMemberList = (await this.GetDBClusterMemberListAsync(clusterId))
            .Where(m => m.IsClusterWriter.HasValue && m.IsClusterWriter.Value)
            .ToList();

        if (matchedMemberList.Count == 0)
        {
            throw new Exception($"Cannot find writer instance in cluster {clusterId}");
        }

        return matchedMemberList[0].DBInstanceIdentifier;
    }

    public async Task<string> GetRandomDBClusterReaderInstanceIdAsync(string clusterId)
    {
        var matchedMemberList = (await this.GetDBClusterMemberListAsync(clusterId))
            .Where(m => !m.IsClusterWriter.HasValue || !m.IsClusterWriter.Value)
            .ToList();

        if (matchedMemberList.Count == 0)
        {
            throw new Exception($"Cannot find a reader instance in cluster {clusterId}");
        }

        int index = new Random().Next(matchedMemberList.Count);
        return matchedMemberList[index].DBInstanceIdentifier;
    }

    public async Task CrashInstance(string instanceId, TaskCompletionSource tcs)
    {
        var deployment = TestEnvironment.Env.Info.Request.Deployment;

        if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER)
        {
            var simulationTask = this.SimulateTemporaryFailureTask(instanceId, TimeSpan.Zero, TimeSpan.FromSeconds(12), tcs);
        }
        else
        {
            try
            {
                var clusterId = TestEnvironment.Env.Info.RdsDbName!;
                await this.FailoverClusterToATargetAndWaitUntilWriterChanged(
                    clusterId,
                    await this.GetDBClusterWriterInstanceIdAsync(clusterId),
                    await this.GetRandomDBClusterReaderInstanceIdAsync(clusterId));
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
                throw;
            }

            tcs.TrySetResult();
        }
    }

    public Task SimulateTemporaryFailureTask(string instanceName, TimeSpan delay, TimeSpan duration, TaskCompletionSource tcs)
    {
        return Task.Run(async () =>
        {
            try
            {
                Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Simulating temporary failure to {instanceName}...");
                if (delay != TimeSpan.Zero)
                {
                    await Task.Delay(delay);
                }

                await ProxyHelper.DisableConnectivityAsync(instanceName);
                await Task.Delay(2000);
                tcs.TrySetResult();

                await Task.Delay(duration);
                await ProxyHelper.EnableConnectivityAsync(instanceName);
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
                throw;
            }
        });
    }

    public async Task FailoverClusterToATargetAndWaitUntilWriterChanged(string clusterId, string initialWriterId, string targetWriterId)
    {
        var deployment = TestEnvironment.Env.Info.Request.Deployment;
        var clusterEndpoint = TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpoint;

        if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE)
        {
            throw new NotSupportedException("Failover is not supported for " + deployment);
        }

        await this.FailoverClusterToTargetAsync(clusterId, targetWriterId);

        var clusterIp = this.HostToIP(clusterEndpoint, true);

        // Failover has finished, wait for DNS to be updated so cluster endpoint resolves to the correct writer instance.
        if (deployment == DatabaseEngineDeployment.AURORA)
        {
            Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Cluster endpoint resolves to: {clusterIp}");
            var newClusterIp = this.HostToIP(clusterEndpoint, true);

            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(10);
            while (clusterIp != null
                   && clusterIp == newClusterIp
                   && DateTime.UtcNow < deadline)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                newClusterIp = this.HostToIP(clusterEndpoint, true);
            }

            Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Cluster endpoint resolves to (after wait): {newClusterIp}");

            // Wait for initial writer instance to be verified as not writer.
            deadline = DateTime.UtcNow + TimeSpan.FromMinutes(10);
            while (await this.IsDBInstanceWriterAsync(initialWriterId)
                && DateTime.UtcNow < deadline)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
            }

            var instancesWithoutInitialWriter = TestEnvironment.Env.Info.DatabaseInfo.Instances.Where(i => i.InstanceId != initialWriterId).ToList();

            await this.MakeSureInstancesUpAsync(instancesWithoutInitialWriter, TimeSpan.FromMinutes(5));
        }

        Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Finished failover from {initialWriterId} to target: {targetWriterId}");
    }

    public async Task FailoverClusterToTargetAsync(string clusterId, string? targetInstanceId)
    {
        await this.WaitUntilClusterHasRightStateAsync(clusterId);

        int remainingAttempts = 10;
        while (--remainingAttempts > 0)
        {
            try
            {
                Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Sending FailoverDbCluster request...");
                var response = await this.rdsClient.FailoverDBClusterAsync(new FailoverDBClusterRequest
                {
                    DBClusterIdentifier = clusterId,
                    TargetDBInstanceIdentifier = targetInstanceId,
                });
                Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} FailoverDbCluster request is sent");
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} FailoverDBCluster request to {targetInstanceId} failed: {ex.Message}");
                await Task.Delay(1000);
            }
        }

        throw new Exception($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Failed to request a cluster failover.");
    }

    public string GetInstanceIdSql(DatabaseEngine engine, DatabaseEngineDeployment deployment)
    {
        return deployment switch
        {
            DatabaseEngineDeployment.AURORA => engine switch
            {
                DatabaseEngine.MYSQL => "SELECT @@aurora_server_id as id",
                DatabaseEngine.PG => "SELECT aurora_db_instance_identifier()",
                _ => throw new NotSupportedException($"Unsupported database engine: {engine}"),
            },
            DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER => engine switch
            {
                DatabaseEngine.MYSQL => "SELECT SUBSTRING_INDEX(endpoint, '.', 1) as id FROM mysql.rds_topology WHERE id=@@server_id",
                DatabaseEngine.PG => "SELECT SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint)) as id "
                                     + "FROM rds_tools.show_topology() "
                                     + "WHERE id IN (SELECT dbi_resource_id FROM rds_tools.dbi_resource_id())",
                _ => throw new NotSupportedException($"Unsupported database engine: {engine}"),
            },
            _ => throw new NotSupportedException($"Unsupported database deployment: {deployment}"),
        };
    }

    public async Task<string> ExecuteQuery(DbConnection connection, DatabaseEngine engine, DatabaseEngineDeployment deployment, string query, bool async)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = query;
        string? result;
        if (async)
        {
            result = Convert.ToString(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        }
        else
        {
            result = Convert.ToString(command.ExecuteScalar());
        }

        if (result == null)
        {
            throw new InvalidOperationException("Failed to retrieve instance ID.");
        }

        Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Finished ExecuteScalar with result: {result}");
        return result;
    }

    public async Task<string?> ExecuteInstanceIdQuery(DbConnection connection, DatabaseEngine engine, DatabaseEngineDeployment deployment, bool async)
    {
        try
        {
            return await this.ExecuteQuery(connection, engine, deployment, this.GetInstanceIdSql(engine, deployment), async);
        }
        catch (NotSupportedException ex)
        {
            Console.WriteLine("[Warning] error thrown when executing instance id query: ", ex);
            return await Task.FromResult<string?>(null);
        }
    }

    public async Task OpenDbConnection(DbConnection connection, bool async)
    {
        if (async)
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
        }
        else
        {
            connection.Open();
        }
    }

    public async Task<object?> ExecuteScalar(DbCommand command, bool async)
    {
        if (async)
        {
            return await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        }
        else
        {
            return command.ExecuteScalar();
        }
    }

    public string? QueryInstanceId(DbConnection connection)
    {
        return this.ExecuteInstanceIdQuery(
            connection,
            TestEnvironment.Env.Info.Request.Engine,
            TestEnvironment.Env.Info.Request.Deployment,
            async: false).GetAwaiter().GetResult();
    }

    public string GetSleepSql(DatabaseEngine engine, int seconds)
    {
        return engine switch
        {
            DatabaseEngine.MYSQL => $"SELECT sleep({seconds})",
            DatabaseEngine.PG => $"SELECT pg_sleep({seconds})",
            _ => throw new NotSupportedException($"Unsupported database engine: {engine}"),
        };
    }
}
