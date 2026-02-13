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
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
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
