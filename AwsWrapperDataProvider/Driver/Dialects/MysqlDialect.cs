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
using AwsWrapperDataProvider.Driver.HostListProviders;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class MysqlDialect : IDialect
{
    public int DefaultPort { get; } = 3306;

    public string HostAliasQuery { get; } = "SELECT CONCAT(@@hostname, ':', @@port)";

    public string ServerVersionQuery { get; } = "SHOW VARIABLES LIKE 'version_comment'";

    public IList<Type> DialectUpdateCandidates { get; } =
    [
        typeof(AuroraMysqlDialect),
        typeof(RdsMysqlDialect),
    ];

    public HostListProviderSupplier HostListProviderSupplier { get; } = (
        Dictionary<string, string> props,
        IHostListProviderService hostListProviderService,
        IPluginService pluginService) => new ConnectionStringHostListProvider(props, hostListProviderService);

    public bool IsDialect(IDbConnection conn)
    {
        IDbCommand? command = null;
        DbDataReader? reader = null;

        try
        {
            command = conn.CreateCommand();
            command.CommandText = this.ServerVersionQuery;
            reader = (DbDataReader)command.ExecuteReader();

            while (reader.Read())
            {
                int columnCount = reader.FieldCount;
                for (int i = 0; i < columnCount; i++)
                {
                    string? columnValue = reader.IsDBNull(i) ? null : reader.GetString(i);
                    if (columnValue != null && columnValue.ToLower().Contains("mysql"))
                    {
                        return true;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            // ignored
        }
        finally
        {
            reader?.Close();
            command?.Dispose();
        }

        return false;
    }

    public void PrepareConnectionProperties(Dictionary<string, string> props, HostSpec hostSpec)
    {
        // Do nothing.
    }
}
