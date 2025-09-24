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

using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo.HostSelectors;
using AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Driver.Plugins.FederatedAuth;
using AwsWrapperDataProvider.Driver.Plugins.Iam;
using AwsWrapperDataProvider.Driver.Plugins.SecretsManager;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Tests.Container.Utils;

[assembly: CollectionBehavior(DisableTestParallelization = true)]
[assembly: CaptureConsole]

namespace AwsWrapperDataProvider.Tests;

public abstract class IntegrationTestBase : IAsyncLifetime
{
    protected static readonly AuroraTestUtils AuroraUtils = AuroraTestUtils.GetUtility();

    protected static readonly string DefaultDbName = TestEnvironment.Env.Info.DatabaseInfo.DefaultDbName;
    protected static readonly string Username = TestEnvironment.Env.Info.DatabaseInfo.Username;
    protected static readonly string Password = TestEnvironment.Env.Info.DatabaseInfo.Password;
    protected static readonly DatabaseEngine Engine = TestEnvironment.Env.Info.Request.Engine;
    protected static readonly DatabaseEngineDeployment Deployment = TestEnvironment.Env.Info.Request.Deployment;
    protected static readonly TestProxyDatabaseInfo ProxyDatabaseInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!;
    protected static readonly string ProxyClusterEndpoint = ProxyDatabaseInfo.ClusterEndpoint;
    protected static readonly int ProxyPort = ProxyDatabaseInfo.ClusterEndpointPort;
    protected static readonly int NumberOfInstances = TestEnvironment.Env.Info.DatabaseInfo.Instances.Count;

    protected static readonly string Endpoint = Deployment switch
    {
        DatabaseEngineDeployment.AURORA => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpoint,
        DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpoint,
        DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE => TestEnvironment.Env.Info.DatabaseInfo.Instances[0].Host,
        _ => throw new InvalidOperationException($"Unsupported deployment {Deployment}"),
    };

    protected static readonly int Port = Deployment switch
    {
        DatabaseEngineDeployment.AURORA => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpointPort,
        DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpointPort,
        DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE => TestEnvironment.Env.Info.DatabaseInfo.Instances[0].Port,
        _ => throw new InvalidOperationException($"Unsupported deployment {Deployment}"),
    };

    protected virtual bool MakeSureFirstInstanceWriter => false;

    public async ValueTask InitializeAsync()
    {
        if (TestEnvironment.Env.Info.Request.Features.Contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED))
        {
            await ProxyHelper.EnableAllConnectivityAsync();
            await ProxyHelper.ClearAllLatencyAsync();
        }

        var deployment = TestEnvironment.Env.Info.Request.Deployment;
        if (deployment == DatabaseEngineDeployment.AURORA || deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER)
        {
            int remainingTries = 3;
            bool success = false;

            while (remainingTries-- > 0 && !success)
            {
                try
                {
                    await TestEnvironment.CheckClusterHealthAsync(this.MakeSureFirstInstanceWriter);
                    success = true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Cluster {TestEnvironment.Env.Info.RdsDbName} is not healthy: {ex.Message}. Rebooting all instances and retrying...");

                    switch (deployment)
                    {
                        case DatabaseEngineDeployment.AURORA:
                            await TestEnvironment.RebootAllClusterInstancesAsync();
                            break;
                        case DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER:
                            await TestEnvironment.RebootClusterAsync();
                            break;
                        default:
                            throw new InvalidOperationException($"Unsupported deployment {deployment}");
                    }

                    Console.WriteLine($"Remaining attempts: {remainingTries}");
                }
            }

            if (!success)
            {
                throw new Exception($"Cluster {TestEnvironment.Env.Info.RdsDbName} is not healthy.");
            }

            Console.WriteLine($"Cluster {TestEnvironment.Env.Info.RdsDbName} is healthy.");
        }
    }

    public ValueTask DisposeAsync()
    {
        Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Clearing all cache for each integration test.");
        RdsHostListProvider.ClearAll();
        MonitoringRdsHostListProvider.CloseAllMonitors();
        HostMonitorService.CloseAllMonitors();
        PluginService.ClearCache();
        DialectProvider.ResetEndpointCache();
        SecretsManagerAuthPlugin.ClearCache();
        FederatedAuthPlugin.ClearCache();
        IamAuthPlugin.ClearCache();
        OktaAuthPlugin.ClearCache();
        RoundRobinHostSelector.ClearCache();
        Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Done Clearing all cache for each integration test.");
        return ValueTask.CompletedTask;
    }
}
