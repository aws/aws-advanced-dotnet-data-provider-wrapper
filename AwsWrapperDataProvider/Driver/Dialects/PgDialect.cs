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
using System.Linq.Expressions;
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class PgDialect : IDialect
{
    public int DefaultPort { get; } = 5432;

    // public IExceptionHandler ExceptionHandler { get; }

    public string HostAliasQuery { get; } = "SELECT CONCAT(inet_server_addr(), ':', inet_server_port())";

    public string ServerVersionQuery { get; } = "SELECT 'version', VERSION()";

    public IList<string> DialectUpdateCandidates { get; } = new List<string>()
    {
        DialectCodes.AURORAPG,
    };

    public HostListProviderSupplier HostListProviderSupplier { get; } = (
        Dictionary<string, string> props,
        string? initialUrl,
        IHostListProviderService hostListProviderService,
        IPluginService pluginService) => new ConnectionStringHostListProvider(props, initialUrl, hostListProviderService);

    public bool IsDialect(DbConnection conn)
    {
        throw new NotImplementedException();
    }

    public void PrepareConnectionProperties(Dictionary<string, string> connectionpProps, HostSpec hostSpec)
    {
        throw new NotImplementedException();
    }
}
