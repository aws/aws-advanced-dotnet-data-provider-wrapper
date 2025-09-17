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

    public IList<HostSpec> Refresh()
    {
        this.Init();
        return this.hostList.AsReadOnly();
    }

    public IList<HostSpec> Refresh(IDbConnection connection)
    {
        return this.Refresh();
    }

    public IList<HostSpec> ForceRefresh()
    {
        this.Init();
        return this.hostList.AsReadOnly();
    }

    public IList<HostSpec> ForceRefresh(IDbConnection connection)
    {
        return this.ForceRefresh();
    }

    public HostRole GetHostRole(IDbConnection connection)
    {
        throw new NotSupportedException("ConnectionStringHostListProvider does not support GetHostRole.");
    }

    public string GetClusterId()
    {
        throw new NotSupportedException("ConnectionStringHostListProvider does not support GetClusterId.");
    }

    public HostSpec? IdentifyConnection(DbConnection connection, DbTransaction? transaction = null)
    {
        // TODO Log unsupported operation.
        return null;
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
            // TODO: move error string to resx file.
            throw new ArgumentException("Connection string is invalid.", nameof(this.properties));
        }

        this.hostListProviderService.InitialConnectionHostSpec = this.hostList.First();
        this.isInitialized = true;
    }
}
