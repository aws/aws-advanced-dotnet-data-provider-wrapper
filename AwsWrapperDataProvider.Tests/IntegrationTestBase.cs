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

using AwsWrapperDataProvider.Dialect.MySqlClient;
using AwsWrapperDataProvider.Dialect.MySqlConnector;
using AwsWrapperDataProvider.Dialect.Npgsql;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo.HostSelectors;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.AuroraConnectionTracker;
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Plugin.FederatedAuth.FederatedAuth;
using AwsWrapperDataProvider.Plugin.Iam.Iam;
using AwsWrapperDataProvider.Plugin.SecretsManager.SecretsManager;
using AwsWrapperDataProvider.Telemetry.XRay;
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
    protected static readonly TestProxyDatabaseInfo? ProxyDatabaseInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo;
    protected static readonly string ProxyClusterEndpoint = ProxyDatabaseInfo?.ClusterEndpoint ?? string.Empty;
    protected static readonly int ProxyPort = ProxyDatabaseInfo?.ClusterEndpointPort ?? 0;
    protected static readonly int NumberOfInstances = TestEnvironment.Env.Info.DatabaseInfo.Instances.Count;

    protected static readonly string Endpoint = Deployment switch
    {
        DatabaseEngineDeployment.AURORA => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpoint,
        DatabaseEngineDeployment.AURORA_LIMITLESS => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpoint,
        DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpoint,
        DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE => TestEnvironment.Env.Info.DatabaseInfo.Instances[0].Host,
        _ => throw new InvalidOperationException($"Unsupported deployment {Deployment}"),
    };

    protected static readonly int Port = Deployment switch
    {
        DatabaseEngineDeployment.AURORA => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpointPort,
        DatabaseEngineDeployment.AURORA_LIMITLESS => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpointPort,
        DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpointPort,
        DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE => TestEnvironment.Env.Info.DatabaseInfo.Instances[0].Port,
        _ => throw new InvalidOperationException($"Unsupported deployment {Deployment}"),
    };

    protected virtual bool MakeSureFirstInstanceWriter => false;

    public virtual async ValueTask InitializeAsync()
    {
        MySqlClientDialectLoader.Load();
        MySqlConnectorDialectLoader.Load();
        NpgsqlDialectLoader.Load();

        // Make XRAY resolvable by DefaultTelemetryFactory and bootstrap the
        // SDK side (X-Ray daemon address + OpenTelemetry MeterProvider) so
        // the wrapper's spans/metrics leave the test container.
        // Both calls are idempotent and Setup no-ops when neither
        // TELEMETRY_TRACES_ENABLED nor TELEMETRY_METRICS_ENABLED is set.
        XRayTelemetryLoader.Load();
        TestTelemetry.Setup(TestEnvironment.Env.Info);

        // Loading Aws Authentication Plugins to Plugin Chain.
        ConnectionPluginChainBuilder.RegisterPluginFactory<IamAuthPluginFactory>(PluginCodes.Iam);
        ConnectionPluginChainBuilder.RegisterPluginFactory<FederatedAuthPluginFactory>(PluginCodes.FederatedAuth);
        ConnectionPluginChainBuilder.RegisterPluginFactory<OktaAuthPluginFactory>(PluginCodes.Okta);
        ConnectionPluginChainBuilder.RegisterPluginFactory<SecretsManagerAuthPluginFactory>(PluginCodes.SecretsManager);

        if (TestEnvironment.Env.Info.Request.Features.Contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED))
        {
            await ProxyHelper.EnableAllConnectivityAsync();
            await ProxyHelper.ClearAllLatencyAsync();
        }

        var deployment = TestEnvironment.Env.Info.Request.Deployment;
        if (deployment is DatabaseEngineDeployment.AURORA or DatabaseEngineDeployment.AURORA_LIMITLESS or DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER)
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
                        case DatabaseEngineDeployment.AURORA_LIMITLESS:
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

        if (TestEnvironment.Env.Info.Request.Features.Contains(TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT))
        {
            if (!TestEnvironment.IsBlueGreenDeploymentAvailableForCluster(TestEnvironment.Env.Info.RdsDbName))
            {
                try
                {
                    TestEnvironment.Env.Info.BlueGreenDeploymentId = TestEnvironment.CreateBlueGreenDeployment(AuroraTestUtils.GetUtility()).Result;
                    Console.WriteLine(@"Created Blue Green Deployment Environment");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($@"Failed to create Blue Green Deployment Environment due to exception: {ex.Message}");
                }
            }
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
        OpenedConnectionTracker.ReleaseResources();
        Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Done Clearing all cache for each integration test.");
        return ValueTask.CompletedTask;
    }
}
