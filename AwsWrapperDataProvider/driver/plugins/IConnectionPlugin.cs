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
using AwsWrapperDataProvider.driver.hostInfo;

namespace AwsWrapperDataProvider.driver.plugins;

public interface IConnectionPlugin
{
    ISet<string> GetSubscribeMethods();

    /// <summary>
    /// Established a connection to the given host using the given driver protocol and properties.
    /// TODO: Add params
    /// </summary>
    /// <param name="methodInvokedOn"></param>
    /// <param name="methodName"></param>
    /// <param name="jdbcCallable"></param>
    /// <param name="jdbcMethodArgs"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    T Execute<T>(
        object methodInvokedOn,
        string methodName,
        JdbcCallable<T> jdbcCallable,
        object[] jdbcMethodArgs
    );
    
    void Execute(
        object methodInvokedOn,
        string methodName,
        JdbcCallable jdbcCallable,
        object[] jdbcMethodArgs
    );

    /// <summary>
    /// TODO: Add description
    /// </summary>
    /// <param name="driverProtocol"></param>
    /// <param name="hostSpec"></param>
    /// <param name="props"></param>
    /// <param name="isInitialConnection"></param>
    /// <param name="jdbcCallable"></param>
    /// <returns></returns>
    DbConnection Connect(
        HostSpec hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        JdbcCallable<DbConnection> jdbcCallable
    );

    /// <summary>
    /// TODO: Add description
    /// </summary>
    /// <param name="driverProtocol"></param>
    /// <param name="hostSpec"></param>
    /// <param name="props"></param>
    /// <param name="isInitialConnection"></param>
    /// <param name="forceConnectJdbcCallable"></param>
    /// <returns></returns>
    DbConnection ForceConnect(
        HostSpec hostSpec,
        Dictionary<string, string> props,
        bool isInitialConnection,
        JdbcCallable<DbConnection> forceConnectJdbcCallable
    );

    // bool acceptsStrategy(HostRole role, string strategy);

    // HostSpec getHostSpecByStrategy(HostRole role, string strategy);

    // HostSpec getHostSpecByStrategy(IList<HostSpec> hosts, HostRole role, string strategy);

    /// <summary>
    /// TODO: Add description
    /// </summary>
    /// <param name="driverProtocol"></param>
    /// <param name="initialUrl"></param>
    /// <param name="props"></param>
    /// <param name="hostListProviderService"></param>
    /// <param name="initHostProviderFunc"></param>
    void InitHostProvider(
        string initialUrl,
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService,
        JdbcCallable<Action<object[]>> initHostProviderFunc
    );

    // OldConnectionSuggestedAction notifyConnectionChanged(NodeChangeOptions changes);

    // void notifyNodeListChanged(IDictionary<string, NodeChangeOptions> changes);
}

public delegate T JdbcCallable<out T>(object[] args);

public delegate void JdbcCallable(object[] args);