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
using AwsWrapperDataProvider.Driver.HostListProviders;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class MysqlDialect : IDialect
{
    public int DefaultPort { get; } = 3306;

    public string HostAliasQuery { get; } = "SELECT CONCAT(@@hostname, ':', @@port)";

    public string ServerVersionQuery { get; } = "SHOW VARIABLES LIKE 'version_comment'";

    public IList<DialectCodes> DialectUpdateCandidates { get; } =
    [
        DialectCodes.RdsMultiAzMysqlCluster,
        DialectCodes.AuroraMysql,
        DialectCodes.RdsMysql,
    ];

    public HostListProviderSupplier HostListProviderSupplier { get; } = (
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService,
        IPluginService pluginService) => new ConnectionStringHostListProvider(props, hostListProviderService);

    public bool IsDialect(DbConnection conn)
    {
        throw new NotImplementedException();
    }

    public void PrepareConnectionProperties(Dictionary<string, string> props, HostSpec hostSpec)
    {
        throw new NotImplementedException();
    }
}
