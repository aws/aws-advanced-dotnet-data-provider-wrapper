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

using System.Data.Common;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using Moq;
using MySqlConnector;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

internal class MockFailoverPluginFactory : IConnectionPluginFactory
{
    // Return a mock FailoverPlugin that always returns a new MySqlConnection
    public IConnectionPlugin GetInstance(IPluginService pluginService, Dictionary<string, string> props)
    {
        var mock = new Mock<FailoverPlugin>(pluginService, props) { CallBase = true };
        mock.Setup(m => m.OpenConnection(It.IsAny<HostSpec>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<bool>(), It.IsAny<ADONetDelegate<DbConnection>>()))
            .Returns(new MySqlConnection());
        return mock.Object;
    }
}
