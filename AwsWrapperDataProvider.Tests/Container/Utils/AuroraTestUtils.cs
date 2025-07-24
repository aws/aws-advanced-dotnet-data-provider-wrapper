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
using Amazon;
using Amazon.EC2;
using Amazon.EC2.Model;
using Amazon.RDS;
using Amazon.RDS.Model;
using Amazon.Runtime;
using Amazon.Runtime.Credentials;

namespace AwsWrapperDataProvider.Tests.Container.Utils;
public class AuroraTestUtils
{
    private readonly AmazonRDSClient rdsClient;
    private readonly AmazonEC2Client ec2Client;

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
        this.ec2Client = new AmazonEC2Client(credentials, region);
    }

    public static AuroraTestUtils GetUtility()
    {
        return GetUtility(null);
    }

    public static AuroraTestUtils GetUtility(TestEnvironmentInfo? info)
    {
        if (info == null)
        {
            info = TestEnvironment.Env.Info;
        }

        return new AuroraTestUtils(info.Region!, info.RdsEndpoint);
    }

    private static RegionEndpoint GetRegionInternal(string rdsRegion)
    {
        RegionEndpoint? region = RegionEndpoint.EnumerableAllRegions.FirstOrDefault(r => string.Equals(r.SystemName, rdsRegion, StringComparison.OrdinalIgnoreCase));

        return region != null ? region : throw new ArgumentException($"Unknown AWS region '{rdsRegion}'.");
    }

    public async Task WaitUntilClusterHasRightStateAsync(string clusterId)
    {
        await this.WaitUntilClusterHasRightStateAsync(clusterId, "available");
    }

    public async Task WaitUntilClusterHasRightStateAsync(string clusterId, params string[] allowedStatuses)
    {
        var allowedStatusSet = new HashSet<string>(allowedStatuses.Select(s => s.ToLower()));
        var waitUntil = DateTime.UtcNow.AddMinutes(15);

        string? status = (await this.GetDBClusterAsync(clusterId))?.Status;
        Console.WriteLine($"Cluster status: {status}, waiting for: {string.Join(", ", allowedStatuses)}");

        while (!allowedStatusSet.Contains(status?.ToLower()) && DateTime.UtcNow < waitUntil)
        {
            await Task.Delay(1000);
            var tmpStatus = (await this.GetDBClusterAsync(clusterId))?.Status;
            if (!string.Equals(tmpStatus, status, StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"Cluster status (waiting): {tmpStatus}");
            }

            status = tmpStatus;
        }

        Console.WriteLine($"Cluster status (after wait): {status}");
    }


    public async Task MakeSureInstancesUpAsync(TimeSpan timeout)
    {
        var envInfo = TestEnvironment.Env.Info;
        List<TestInstanceInfo> instances = [.. envInfo.DatabaseInfo!.Instances, .. envInfo.ProxyDatabaseInfo!.Instances];
        await this.MakeSureInstancesUpAsync(instances, timeout);
    }

    public async Task MakeSureInstancesUpAsync(List<TestInstanceInfo> instances, TimeSpan timeout)
    {
        var remainingInstances = new ConcurrentDictionary<string, bool>();
        var dbName = TestEnvironment.Env.Info.DatabaseInfo!.DefaultDbName;
        var username = TestEnvironment.Env.Info.DatabaseInfo!.Username;
        var password = TestEnvironment.Env.Info.DatabaseInfo!.Password;

        foreach (var instance in instances)
        {
            remainingInstances[instance.Host] = true;
        }

        var tasks = new List<Task>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeout));
        var connectTimeout = 30;
        var socketTimeout = 30;

        foreach (var instance in instances)
        {
            tasks.Add(Task.Run(async () =>
            {
                var host = instance.Host;
                var port = instance.Port;
                var url = ConnectionStringHelper.GetUrl(host, port, username, password, dbName);

                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        using var connection = new SqlConnection(url); // Replace with NpgsqlConnection if PostgreSQL
                        connection.ConnectionTimeout = connectTimeout;
                        await connection.OpenAsync(cts.Token);

                        Console.WriteLine($"Host {host} is up.");
                        if (host.Contains(".proxied"))
                        {
                            Console.WriteLine($"Proxied host {host} resolves to IP address {HostToIP(host)}");
                        }

                        remainingInstances.TryRemove(host, out _);
                        break;
                    }
                    catch (DbException ex)
                    {
                        Console.WriteLine($"Retrying connection to {host}: {ex.Message}");
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

        await Task.WhenAll(tasks);

        if (!remainingInstances.IsEmpty)
        {
            throw new Exception("The following instances are still down:\n" + string.Join("\n", remainingInstances.Keys));
        }
    }

    private string HostToIP(string host)
    {
        // Implement DNS resolution if needed
        return System.Net.Dns.GetHostAddresses(host).FirstOrDefault()?.ToString() ?? "Unknown";
    }

    public async Task<DBCluster?> GetDBClusterAsync(string clusterId)
    {
        int remainingTries = 5;
        for (int i = 0; i < remainingTries; i++)
        {
            try
            {
                var response = await this.rdsClient.DescribeDBClustersAsync(
                    new DescribeDBClustersRequest
                {
                    DBClusterIdentifier = clusterId,
                });

                return response.DBClusters.First();
            }
            catch (DBClusterNotFoundException)
            {
                return null;
            }
            catch (AmazonServiceException) when (i < 4)
            {
                // Retry
            }
        }

        throw new Exception($"Unable to get DB cluster info for cluster with ID {clusterId}");
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
}
