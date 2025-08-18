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

using AwsWrapperDataProvider.Tests.Container.Utils;

namespace AwsWrapperDataProvider.Tests;

public class EfmConnectivityIntegrationTests : IntegrationTestBase
{
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    public async Task EfmPluginTest_WithBasicAuth()
    {
        var connectionString = ConnectionStringHelper.GetUrl(this.engine, this.clusterEndpoint, this.port, this.username, this.password, this.defaultDbName);
        connectionString += "; Plugins=efm;";
        await EfmConnectivityTests.PerformEfmTest(connectionString, this.clusterEndpoint);
    }
}
