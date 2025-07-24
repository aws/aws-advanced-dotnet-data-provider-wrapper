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

using System.Text.Json;
using Toxiproxy.Net;

namespace AwsWrapperDataProvider.Tests.Container.Utils;

public class TestEnvironment
{
    private static readonly Lazy<TestEnvironment> LazyTestEnvironmentInstance = new(Create);

    public static TestEnvironment Env = LazyTestEnvironmentInstance.Value;

    public TestEnvironmentInfo Info { get; private set; }

    private Dictionary<string, Proxy> proxies;
    private TestEnvironment() { }

    public IReadOnlyCollection<Proxy> Proxies => this.proxies.Values;


    public async Task CheckClusterHealthAsync(bool makeSureFirstInstanceWriter)
    {
        var testInfo = TestEnvironment.Env.Info;
        var testRequest = testInfo.Request;

        AuroraTestUtils auroraUtil = AuroraTestUtils.GetUtility(testInfo);
        await auroraUtil.WaitUntilClusterHasRightStateAsync(testInfo.RdsDbName!);

        await auroraUtil.MakeSureInstancesUpAsync(TimeSpan.FromMinutes(3));

        if (makeSureFirstInstanceWriter)
        {
            var instanceIDs = new List<string>();
            var startTime = DateTime.UtcNow;

            while ((instanceIDs.Count != testRequest.NumOfInstances ||
                    instanceIDs.Count == 0 ||
                    !await auroraUtil.IsDBInstanceWriterAsync(instanceIDs[0])) &&
                   DateTime.UtcNow - startTime < TimeSpan.FromMinutes(10))
            {
                await Task.Delay(5000);

                try
                {
                    instanceIDs = await auroraUtil.GetAuroraInstanceIdsAsync();
                }
                catch (SqlException ex)
                {
                    if (ex.State.ToString() == POSTGRES_AUTH_ERROR_CODE)
                        throw;

                    instanceIDs = new List<string>();
                }
            }

            if (instanceIDs.Count == 0 || !await auroraUtil.IsDBInstanceWriterAsync(testInfo.RdsDbName, instanceIDs[0]))
                throw new Exception("Writer instance validation failed.");

            var currentWriter = instanceIDs[0];

            var dbInfo = testInfo.DatabaseInfo;
            dbInfo.MoveInstanceFirst(currentWriter);
            testInfo.ProxyDatabaseInfo.MoveInstanceFirst(currentWriter);

            var dnsOk = await auroraUtil.WaitDnsEqualAsync(
                dbInfo.ClusterEndpoint,
                dbInfo.Instances[0].Host,
                TimeSpan.FromMinutes(5),
                false);

            if (!dnsOk)
                throw new Exception("Cluster endpoint isn't updated.");

            if (instanceIDs.Count > 1)
            {
                var dnsROOk = await auroraUtil.WaitDnsNotEqualAsync(
                    dbInfo.ClusterReadOnlyEndpoint,
                    dbInfo.Instances[0].Host,
                    TimeSpan.FromMinutes(5),
                    false);

                if (!dnsROOk)
                    throw new Exception("Cluster RO endpoint isn't updated.");
            }
        }
    }

    private static TestEnvironment Create()
    {
        TestEnvironment env = new();
        string infoJson = Environment.GetEnvironmentVariable("TEST_ENV_INFO_JSON") ?? throw new Exception("Environment variable TEST_ENV_INFO_JSON is required.");

        try
        {
            JsonSerializerOptions options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
            };
            env.Info = JsonSerializer.Deserialize<TestEnvironmentInfo>(infoJson, options) ?? throw new Exception("Deserialized TestEnvironmentInfo is null.");
        }
        catch (JsonException ex)
        {
            throw new Exception($"Failed to deserialize TEST_ENV_INFO_JSON: {ex.Message}", ex);
        }
        catch (Exception ex)
        {
            throw new Exception($"An error occurred while processing TEST_ENV_INFO_JSON: {ex.Message}", ex);
        }

        if (env.Info.Request!.Features.Contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED))
        {
            InitProxies(env);
        }

        return env;
    }

    private static void InitProxies(TestEnvironment environment)
    {
        environment.proxies = [];

        int proxyControlPort = environment.Info!.ProxyDatabaseInfo!.ControlPort;
        foreach (var instance in environment.Info.ProxyDatabaseInfo.Instances)
        {
            Connection proxyConnection = new(instance.Host, proxyControlPort);
            IDictionary<string, Proxy> proxies = proxyConnection.Client().All();
            if (proxies.Count == 0)
            {
                throw new Exception($"Proxy for {instance.InstanceId} is not found.");
            }

            environment.proxies[instance.InstanceId] = proxies.First().Value;
        }

        if (!string.IsNullOrEmpty(environment.Info.ProxyDatabaseInfo.ClusterEndpoint))
        {
            var client = new Connection(environment.Info.ProxyDatabaseInfo.ClusterEndpoint, proxyControlPort).Client();
            Proxy proxy = environment.GetProxy(client, environment.Info.DatabaseInfo!.ClusterEndpoint, environment.Info.DatabaseInfo.ClusterEndpointPort);
            environment.proxies[environment.Info.ProxyDatabaseInfo.ClusterEndpoint] = proxy;
        }

        if (!string.IsNullOrEmpty(environment.Info.ProxyDatabaseInfo.ClusterReadOnlyEndpoint))
        {
            var client = new Connection(environment.Info.ProxyDatabaseInfo.ClusterReadOnlyEndpoint, proxyControlPort).Client();
            Proxy proxy = environment.GetProxy(client, environment.Info.DatabaseInfo!.ClusterReadOnlyEndpoint, environment.Info.DatabaseInfo.ClusterReadOnlyEndpointPort);
            environment.proxies[environment.Info.ProxyDatabaseInfo.ClusterReadOnlyEndpoint] = proxy;
        }
    }

    public Proxy GetProxy(string instanceName)
    {
        if (this.proxies.TryGetValue(instanceName, out Proxy? proxy))
        {
            return proxy;
        }

        throw new Exception($"Proxy for {instanceName} not found.");
    }

    private Proxy GetProxy(Client proxyClient, string host, int port)
    {
        string upstream = $"{host}:{port}";
        return proxyClient.FindProxy(upstream);
    }
}
