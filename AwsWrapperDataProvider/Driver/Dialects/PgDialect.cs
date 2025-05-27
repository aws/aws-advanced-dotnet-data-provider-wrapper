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
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class PgDialect : IDialect
{
    private readonly IExceptionHandler _exceptionHandler = new PgExceptionHandler();

    public int DefaultPort { get; } = 5432;

    public string HostAliasQuery { get; } = "SELECT CONCAT(inet_server_addr(), ':', inet_server_port())";

    public string ServerVersionQuery { get; } = "SELECT 'version', VERSION()";

    public IExceptionHandler ExceptionHandler => this._exceptionHandler;

    public virtual IList<Type> DialectUpdateCandidates { get; } =
    [
        typeof(AuroraPgDialect),
        typeof(RdsPgDialect),
    ];

    public HostListProviderSupplier HostListProviderSupplier { get; } = (
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService,
        IPluginService pluginService) => new ConnectionStringHostListProvider(props, hostListProviderService);

    public virtual bool IsDialect(IDbConnection conn)
    {
        try
        {
            using IDbCommand command = conn.CreateCommand();
            command.CommandText = "SELECT 1 FROM pg_proc LIMIT 1";
            using DbDataReader reader = (DbDataReader)command.ExecuteReader();

            if (reader.HasRows)
            {
                return true;
            }
        }
        catch (Exception)
        {
            // ignored
        }

        return false;
    }

    public void PrepareConnectionProperties(Dictionary<string, string> connectionpProps, HostSpec hostSpec)
    {
        // Do nothing.
    }
}
