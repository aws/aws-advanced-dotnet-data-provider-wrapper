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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Driver.HostListProviders;

public class ConnectionStringHostListProvider : IStaticHostListProvider
{
    private readonly List<HostSpec> _hostList = new();
    private readonly Dictionary<string, string> _properties;
    private readonly IHostListProviderService _hostListProviderService;
    private readonly bool _isSingleWriterConnectionString;

    /// <summary>
    /// Check if _hostList has already been initialized. _hostList should only be initialized once.
    /// </summary>
    private bool _isInitialized = false;

    public ConnectionStringHostListProvider(
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService)
    {
        this._properties = props;
        this._hostListProviderService = hostListProviderService;
        this._isSingleWriterConnectionString = PropertyDefinition.SingleWriterConnectionString.GetBoolean(props);
    }

    public IList<HostSpec> Refresh()
    {
        this.Init();
        return this._hostList.AsReadOnly();
    }

    public IList<HostSpec> Refresh(IDbConnection connection)
    {
        return this.Refresh();
    }

    public IList<HostSpec> ForceRefresh()
    {
        this.Init();
        return this._hostList.AsReadOnly();
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

    private void Init()
    {
        if (this._isInitialized)
        {
            return;
        }

        this._hostList.AddRange(ConnectionPropertiesUtils.GetHostsFromProperties(
                this._properties,
                this._hostListProviderService.HostSpecBuilder,
                this._isSingleWriterConnectionString));
        if (this._hostList.Count == 0)
        {
            // TODO: move error string to resx file.
            throw new ArgumentException("Connection string is invalid.", nameof(this._properties));
        }

        this._hostListProviderService.InitialConnectionHostSpec = this._hostList.First();
        this._isInitialized = true;
    }
}
