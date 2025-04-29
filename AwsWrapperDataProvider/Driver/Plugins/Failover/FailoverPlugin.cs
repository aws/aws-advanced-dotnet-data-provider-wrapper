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
using AwsWrapperDataProvider.Driver.HostInfo;

namespace AwsWrapperDataProvider.Driver.Plugins.Failover;

public class FailoverPlugin(IPluginService pluginService, Dictionary<string, string> props) : IConnectionPlugin
{
    private readonly IPluginService pluginService = pluginService;
    private readonly Dictionary<string, string> props = props;

    public DbConnection Connect(HostSpec? hostSpec, Dictionary<string, string> props, bool isInitialConnection, JdbcCallable<DbConnection> jdbcCallable)
    {
        throw new NotImplementedException();
    }

    public T Execute<T>(object methodInvokedOn, string methodName, JdbcCallable<T> jdbcCallable, object[] jdbcMethodArgs)
    {
        throw new NotImplementedException();
    }

    public void Execute(object methodInvokedOn, string methodName, JdbcCallable jdbcCallable, object[] jdbcMethodArgs)
    {
        throw new NotImplementedException();
    }

    public DbConnection ForceConnect(HostSpec hostSpec, Dictionary<string, string> props, bool isInitialConnection, JdbcCallable<DbConnection> forceConnectJdbcCallable)
    {
        throw new NotImplementedException();
    }

    public ISet<string> GetSubscribeMethods()
    {
        throw new NotImplementedException();
    }

    public void InitHostProvider(string initialUrl, Dictionary<string, string> props, IHostListProviderService hostListProviderService, JdbcCallable<Action<object[]>> initHostProviderFunc)
    {
        throw new NotImplementedException();
    }
}
