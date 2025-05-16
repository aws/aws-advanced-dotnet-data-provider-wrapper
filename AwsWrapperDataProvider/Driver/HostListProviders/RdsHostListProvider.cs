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

public class RdsHostListProvider : IDynamicHostListProvider
{
    protected readonly Dictionary<string, string> properties;
    protected readonly IHostListProviderService hostListProviderService;
    protected readonly string topologyQuery;
    protected readonly string nodeIdQuery;
    protected readonly string isReaderQuery;
    protected bool isInitialized = false;
    protected List<HostSpec> hostList = new();
    protected List<HostSpec> initialHostList = new();
    protected HostSpec? initialHostSpec;
    protected HostSpec? clusterInstanceTemplate;
    protected string clusterId = string.Empty;
    protected long refreshRateNano = Convert.ToInt64(PropertyDefinition.ClusterTopologyRefreshRateMS.DefaultValue) * 1000000;

    // A primary clusterId is a clusterId that is based off of a cluster endpoint URL
    // (rather than a GUID or a value provided by the user).
    protected bool isPrimaryClusterId = false;

    public RdsHostListProvider(
        Dictionary<string, string> properties,
        IHostListProviderService hostListProviderService,
        string topologyQuery,
        string nodeIdQuery,
        string isReaderQuery)
    {
        this.properties = properties;
        this.hostListProviderService = hostListProviderService;
        this.topologyQuery = topologyQuery;
        this.nodeIdQuery = nodeIdQuery;
        this.isReaderQuery = isReaderQuery;
    }

    protected void init()
    {
        if (this.isInitialized)
        {
            return;
        }

        this.initialHostList.AddRange(ConnectionPropertiesUtils.GetHostsFromProperties(
                this.properties,
                this.hostListProviderService.HostSpecBuilder,
                false));
        if (this.initialHostList.Count == 0)
        {
            // TODO: move error string to resx file.
            throw new Exception("No hosts found in connection string");
        }

        this.initialHostSpec = this.initialHostList[0];
        this.hostListProviderService.InitialConnectionHostSpec = this.initialHostSpec;
        this.clusterId = Guid.NewGuid().ToString();
        this.isPrimaryClusterId = false;
        this.refreshRateNano = PropertyDefinition.ClusterTopologyRefreshRateMS.GetLong(this.properties)! * 1000000 ?? this.refreshRateNano;

        string? clusterInstancePattern = PropertyDefinition.ClusterInstanceHostPattern.GetString(this.properties);
        if (clusterInstancePattern != null)
        {

        }

    }

    public IList<HostSpec> ForceRefresh()
    {
        throw new NotImplementedException();
    }

    public IList<HostSpec> ForceRefresh(DbConnection connection)
    {
        throw new NotImplementedException();
    }

    public string GetClusterId()
    {
        throw new NotImplementedException();
    }

    public HostRole GetHostRole(DbConnection connection)
    {
        throw new NotImplementedException();
    }

    public IList<HostSpec> Refresh()
    {
        throw new NotImplementedException();
    }

    public IList<HostSpec> Refresh(DbConnection connection)
    {
        throw new NotImplementedException();
    }
}
