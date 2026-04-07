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

using Amazon.RDS.Model;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Tests.Container.Utils;

namespace AwsWrapperDataProvider.Tests;

/// <summary>
/// Fixture that creates a custom DB cluster endpoint before tests and deletes it after.
/// Used only for Aurora with 3+ instances (test is skipped otherwise).
/// </summary>
public class CustomEndpointTestFixture : IDisposable
{
    public CustomEndpointTestFixture()
    {
        this.EndpointId = "test-endpoint-1-" + Guid.NewGuid().ToString("N")[..8];
        this.EndpointInfo = null;

        var envInfo = TestEnvironment.Env.Info;
        if (envInfo.Request.Deployment != DatabaseEngineDeployment.AURORA || envInfo.DatabaseInfo.Instances.Count < 3)
        {
            return;
        }

        var clusterId = envInfo.RdsDbName!;
        var instances = envInfo.DatabaseInfo.Instances;
        var staticMembers = instances.Take(1).Select(i => i.InstanceId).ToList();

        var auroraUtilForCreate = AuroraTestUtils.GetUtility(envInfo);
        auroraUtilForCreate.CreateDBClusterEndpointAsync(this.EndpointId, clusterId, staticMembers).GetAwaiter().GetResult();
        this.EndpointInfo = auroraUtilForCreate.WaitUntilEndpointAvailableAsync(this.EndpointId).GetAwaiter().GetResult();
    }

    public string EndpointId { get; }
    public DBClusterEndpoint? EndpointInfo { get; }

    public async Task SetupCustomEndpointRoleAsync(HostRole hostRole, ITestOutputHelper logger)
    {
        var auroraUtils = AuroraTestUtils.GetUtility();

        logger.WriteLine($"Setting up custom endpoint instance with role: {hostRole}");
        var endpointMembers = this.EndpointInfo!.StaticMembers ?? new List<string>();
        var clusterId = TestEnvironment.Env.Info.RdsDbName!;
        var originalWriter = await auroraUtils.GetDBClusterWriterInstanceIdAsync(clusterId);

        var connectionStringNoPlugins = ConnectionStringHelper.GetUrl(
            TestEnvironment.Env.Info.Request.Engine,
            this.EndpointInfo.Endpoint,
            TestEnvironment.Env.Info.Request.Deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE
                ? TestEnvironment.Env.Info.DatabaseInfo.Instances[0].Port
                : TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpointPort,
            TestEnvironment.Env.Info.DatabaseInfo.Username,
            TestEnvironment.Env.Info.DatabaseInfo.Password,
            TestEnvironment.Env.Info.DatabaseInfo.DefaultDbName,
            10,
            10,
            string.Empty,
            false);

        using (var conn = auroraUtils.CreateAwsWrapperConnection(
            TestEnvironment.Env.Info.Request.Engine, connectionStringNoPlugins))
        {
            await conn.OpenAsync();
            var originalInstanceId = await auroraUtils.QueryInstanceId(conn, true);
            if (!endpointMembers.Contains(originalInstanceId!))
            {
                throw new InvalidOperationException($"Instance {originalInstanceId} should be in endpoint members");
            }

            string? failoverTarget = null;
            if (hostRole == HostRole.Writer)
            {
                if (originalInstanceId == originalWriter)
                {
                    logger.WriteLine($"Role is already {hostRole}, no failover needed.");
                    return;
                }

                failoverTarget = originalInstanceId;
                logger.WriteLine("Failing over to get writer role...");
            }
            else if (hostRole == HostRole.Reader)
            {
                if (originalInstanceId != originalWriter)
                {
                    logger.WriteLine($"Role is already {hostRole}, no failover needed.");
                    return;
                }

                logger.WriteLine("Failing over to get reader role...");
            }

            await auroraUtils.FailoverClusterToATargetAndWaitUntilWriterChanged(
                clusterId, originalWriter, failoverTarget!);

            var originalInstanceInfo = TestEnvironment.Env.Info.DatabaseInfo.Instances
                .Where(i => i.InstanceId == originalInstanceId).ToList();
            await auroraUtils.MakeSureInstancesUpAsync([.. originalInstanceInfo], TimeSpan.FromMinutes(5));
        }

        logger.WriteLine($"Verifying that new connection has role: {hostRole}");
        using (var conn = auroraUtils.CreateAwsWrapperConnection(
            TestEnvironment.Env.Info.Request.Engine, connectionStringNoPlugins))
        {
            await conn.OpenAsync();
            var currentInstanceId = await auroraUtils.QueryInstanceId(conn, true);
            if (!endpointMembers.Contains(currentInstanceId!))
            {
                throw new InvalidOperationException($"Instance {currentInstanceId} should be in endpoint members");
            }

            var newRole = await auroraUtils.QueryHostRoleAsync(
                conn, TestEnvironment.Env.Info.Request.Engine, true);
            if (newRole != hostRole)
            {
                throw new InvalidOperationException($"Expected role {hostRole} but got {newRole}");
            }
        }

        logger.WriteLine($"Custom endpoint instance successfully set to role: {hostRole}");
    }

    public void Dispose()
    {
        if (this.EndpointInfo == null)
        {
            return;
        }

        var auroraUtil = AuroraTestUtils.GetUtility();
        auroraUtil.DeleteDBClusterEndpointAsync(this.EndpointId).GetAwaiter().GetResult();
    }
}
