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
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class MySqlDialect : IDialect
{
    private static readonly ILogger<MySqlDialect> Logger = LoggerUtils.GetLogger<MySqlDialect>();

    public int DefaultPort { get; } = 3306;

    public string HostAliasQuery { get; } = "SELECT CONCAT(@@hostname, ':', @@port)";

    public string ServerVersionQuery { get; } = "SHOW VARIABLES LIKE 'version_comment'";

    public IExceptionHandler ExceptionHandler { get; } = new MySqlExceptionHandler();

    public virtual IList<Type> DialectUpdateCandidates { get; } =
    [
        typeof(RdsMultiAzDbClusterMySqlDialect),
        typeof(AuroraMySqlDialect),
        typeof(RdsMySqlDialect),
    ];

    public virtual HostListProviderSupplier HostListProviderSupplier { get; } = (
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService,
        IPluginService pluginService) => new ConnectionStringHostListProvider(props, hostListProviderService);

    public virtual bool IsDialect(IDbConnection conn)
    {
        try
        {
            using IDbCommand command = conn.CreateCommand();
            command.CommandText = this.ServerVersionQuery;
            using IDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                int columnCount = reader.FieldCount;
                for (int i = 0; i < columnCount; i++)
                {
                    string columnValue = reader.GetString(i);
                    if (columnValue != null && columnValue.Contains("mysql", StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Error occurred when checking whether it's MySQL dialect");
        }

        return false;
    }

    public virtual void PrepareConnectionProperties(Dictionary<string, string> props, HostSpec hostSpec)
    {
        // Do nothing.
    }
}
