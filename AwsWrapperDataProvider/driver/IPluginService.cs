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
using AwsWrapperDataProvider.driver.connectionProviders;
using AwsWrapperDataProvider.driver.dialects;
using AwsWrapperDataProvider.driver.hostInfo;
using AwsWrapperDataProvider.driver.hostListProviders;
using AwsWrapperDataProvider.driver.plugins;
using AwsWrapperDataProvider.driver.targetDriverDialects;

namespace AwsWrapperDataProvider.driver;

public interface IPluginService
{
    
    IDialect Dialect { get; }

    ITargetDriverDialect TargetDriverDialect { get; }
    
    DbConnection? CurrentConnection { get; set; }

    HostSpec GetCurrentHostSpec();

    void SetCurrentConnection(DbConnection connection, HostSpec hostSpec);

    void SetCurrentConnection(DbConnection connection, HostSpec hostSpec, IConnectionPlugin pluginToSkip);

    IList<HostSpec> GetAllHosts();

    IList<HostSpec> GetHosts();

    HostSpec GetInitialConnectionHostSpec();

    // void SetAllowedAndBlockedHosts(AllowedAndBlockedHosts allowedAndBlockedHosts);

    // bool AcceptsStrategy(HostRole role, string strategy);

    // HostSpec GetHostSpecByStrategy(HostSpec hostSpec, string strategy);

    // HostSpec GetHostSpecByStrategy(IList<HostSpec> hosts, HostRole role, string strategy);

    HostRole GetHostRole(DbConnection connection);

    // HostRole SetAvailability(ISet<string> hostAliases, HostAvailability availability);

    // bool IsInTransaction();

    IHostListProvider GetHostListProvider();

    void RefreshHostList();

    // void RefreshHostList(DbConnection connection);

    // void ForceRefreshHostList();

    // void ForceRefreshHostList(DbConnection connection);

    // void ForceRefreshHostList(bool shouldVerifyWriter, long timeoutMs);

    DbConnection Connect(HostSpec hostSpec, Dictionary<string, string> props, IConnectionPlugin pluginToSkip);

    DbConnection ForceConnect(HostSpec hostSpec, Dictionary<string, string> props, IConnectionPlugin pluginToSkip);

    void UpdateDialect(DbConnection connection);

    HostSpec IdentifyConnection(DbConnection connection);

    void FillAliases(DbConnection connection, HostSpec hostSpec);

    HostSpecBuilder GetHostSpecBuilder();

    IConnectionProvider GetConnectionProvider();
}