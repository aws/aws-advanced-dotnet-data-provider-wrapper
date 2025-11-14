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

using System.Data;
using System.Data.Common;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;

namespace AwsWrapperDataProvider.Driver.HostListProviders;

public class ConnectionStringHostListProvider : IStaticHostListProvider
{
    private readonly List<HostSpec> hostList = [];
    private readonly Dictionary<string, string> properties;
    private readonly IHostListProviderService hostListProviderService;
    private readonly bool isSingleWriterConnectionString;

    /// <summary>
    /// Check if hostList has already been initialized. hostList should only be initialized once.
    /// </summary>
    private bool isInitialized = false;

    public ConnectionStringHostListProvider(
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService)
    {
        this.properties = props;
        this.hostListProviderService = hostListProviderService;
        this.isSingleWriterConnectionString = PropertyDefinition.SingleWriterConnectionString.GetBoolean(props);
    }

    public Task<IList<HostSpec>> RefreshAsync()
    {
        this.Init();
        return Task.FromResult((IList<HostSpec>)this.hostList.AsReadOnly());
    }

    public async Task<IList<HostSpec>> RefreshAsync(DbConnection? connection)
    {
        return await this.RefreshAsync();
    }

    public Task<IList<HostSpec>> ForceRefreshAsync()
    {
        this.Init();
        return Task.FromResult((IList<HostSpec>)this.hostList.AsReadOnly());
    }

    public async Task<IList<HostSpec>> ForceRefreshAsync(DbConnection? connection)
    {
        return await this.ForceRefreshAsync();
    }

    public Task<HostRole> GetHostRoleAsync(DbConnection connection)
    {
        throw new NotSupportedException(Resources.Error_ConnectionStringHostListProvider_GetHostRoleAsync);
    }

    public string GetClusterId()
    {
        throw new NotSupportedException(Resources.Error_ConnectionStringHostListProvider_GetClusterId);
    }

    public Task<HostSpec?> IdentifyConnectionAsync(DbConnection connection, DbTransaction? transaction = null)
    {
        // TODO Log unsupported operation.
        return Task.FromResult<HostSpec?>(null);
    }

    private void Init()
    {
        if (this.isInitialized)
        {
            return;
        }

        this.hostList.AddRange(ConnectionPropertiesUtils.GetHostsFromProperties(
                this.properties,
                this.hostListProviderService.HostSpecBuilder,
                this.isSingleWriterConnectionString));
        if (this.hostList.Count == 0)
        {
            throw new ArgumentException(Resources.Error_ConnectionStringInvalid, nameof(this.properties));
        }

        this.hostListProviderService.InitialConnectionHostSpec = this.hostList.First();
        this.isInitialized = true;
    }
}
