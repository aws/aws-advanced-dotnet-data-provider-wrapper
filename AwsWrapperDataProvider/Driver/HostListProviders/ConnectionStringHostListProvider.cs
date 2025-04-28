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
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Driver.HostListProviders;

public class ConnectionStringHostListProvider : IStaticHostListProvider
{
    private readonly List<HostSpec> _hostList = new List<HostSpec>();
    private readonly Dictionary<string, string> _properties;
    private readonly IHostListProviderService _hostListProviderService;

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
    }

    public IList<HostSpec> Refresh()
    {
        this.Init();
        return this._hostList.AsReadOnly();
    }

    public IList<HostSpec> Refresh(DbConnection connection)
    {
        throw new NotImplementedException();
    }

    public IList<HostSpec> ForceRefresh()
    {
        throw new NotImplementedException();
    }

    public IList<HostSpec> ForceRefresh(DbConnection connection)
    {
        throw new NotImplementedException();
    }

    public HostRole GetHostRole(DbConnection connection)
    {
        throw new NotImplementedException();
    }

    public HostSpec GetHostSpec(DbConnection connection)
    {
        throw new NotImplementedException();
    }

    public string GetClusterId()
    {
        throw new NotImplementedException();
    }

    private void Init()
    {
        if (this._isInitialized)
        {
            return;
        }

        this._hostList.AddRange(ConnectionPropertiesUtils.GetHostsFromProperties(
                this._properties,
                this._hostListProviderService.HostSpecBuilder));
        if (this._hostList.Count == 0)
        {
            // TODO: move error string to resx file.
            throw new ArgumentException("Connection string is invalid.", nameof(this._properties));
        }

        this._hostListProviderService.InitialConnectionHostSpec = this._hostList.First();
        this._isInitialized = true;
    }
}
