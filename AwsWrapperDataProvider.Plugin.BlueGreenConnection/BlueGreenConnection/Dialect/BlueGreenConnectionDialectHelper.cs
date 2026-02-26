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
using AwsWrapperDataProvider.Driver.Dialects;

namespace AwsWrapperDataProvider.Plugin.BlueGreenConnection.BlueGreenConnection.Dialect;

public class BlueGreenConnectionDialectHelper
{
    protected static readonly string AuroraMySqlBgTopologyExistsQuery =
        "SELECT 1 AS tmp FROM information_schema.tables WHERE" +
        " table_schema = 'mysql' AND table_name = 'rds_topology'";

    protected static readonly string AuroraMySqlBgStatusQuery = "SELECT * FROM mysql.rds_topology";

    protected static readonly string AuroraPostgreSqlBgTopologyExistsQuery =
        "SELECT 'pg_catalog.get_blue_green_fast_switchover_metadata'::regproc";

    protected static readonly string AuroraPostgreSqlBgStatusQuery =
        "SELECT * FROM " +
        $"pg_catalog.get_blue_green_fast_switchover_metadata('aws_dotnet_driver')";

    protected static readonly string RdsMySqlTopologyTableExistsQuery =
        "SELECT 1 AS tmp FROM information_schema.tables WHERE" +
        " table_schema = 'mysql' AND table_name = 'rds_topology'";

    protected static readonly string RdsMySqlBgStatusQuery = "SELECT * FROM mysql.rds_topology";

    protected static readonly string RdsPgTopologyTableExistsQuery =
        "SELECT 'rds_tools.show_topology'::regproc";

    protected static readonly string RdsPgBgStatusQuery =
        $"SELECT * FROM rds_tools.show_topology('aws_dotnet_driver')";


    public static bool IsBlueGreenStatusAvailable(IDialect dialect, DbConnection connection)
    {
        return dialect switch
        {
            AuroraMySqlDialect => CheckExistenceQueries(connection, AuroraMySqlBgTopologyExistsQuery),
            AuroraPgDialect => CheckExistenceQueries(connection, AuroraPostgreSqlBgTopologyExistsQuery),
            RdsMySqlDialect => CheckExistenceQueries(connection, RdsMySqlTopologyTableExistsQuery),
            RdsPgDialect => CheckExistenceQueries(connection, RdsPgTopologyTableExistsQuery),
            _ => throw new ArgumentException($"Unsupported dialect: {dialect.GetType().Name}"),
        };
    }

    public static string GetBlueGreenStatusQuery(IDialect dialect)
    {
        return dialect switch
        {
            AuroraMySqlDialect _ => AuroraMySqlBgStatusQuery,
            AuroraPgDialect _ => AuroraPostgreSqlBgStatusQuery,
            RdsMySqlDialect _ => RdsMySqlBgStatusQuery,
            RdsPgDialect _ => RdsPgBgStatusQuery,
            _ => throw new ArgumentException($"Unsupported dialect: {dialect.GetType().Name}"),
        };
    }

    public static bool IsBlueGreenConnectionDialect(IDialect dialect)
    {
        return dialect is AuroraMySqlDialect or AuroraPgDialect or RdsMySqlDialect or RdsPgDialect;
    }

    private static bool CheckExistenceQueries(DbConnection conn, params string[] existenceQueries)
    {
        foreach (var existenceQuery in existenceQueries)
        {
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = existenceQuery;
                using var reader = cmd.ExecuteReader();
                if (!reader.Read())
                {
                    return false;
                }
            }
            catch (DbException)
            {
                return false;
            }
        }

        return true;
    }
}
