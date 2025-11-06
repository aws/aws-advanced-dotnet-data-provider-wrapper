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

using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests.Container.Utils;

public class ConnectionStringHelper
{
    public static string GetUrl(DatabaseEngine engine, string host, int? port, string? username, string? password, string? dbName, int commandTimeout = 30, int connectionTimeout = 30, string? plugins = null)
    {
        string url;
        switch (engine)
        {
            case DatabaseEngine.MYSQL:
                MySqlConnectionStringBuilder mySqlConnectionStringBuilder = new();
                mySqlConnectionStringBuilder.Server = host;
                if (port != null & port > 0)
                {
                    mySqlConnectionStringBuilder.Port = (uint)port!;
                }

                if (username != null)
                {
                    mySqlConnectionStringBuilder.UserID = username;
                }

                if (password != null)
                {
                    mySqlConnectionStringBuilder.Password = password;
                }

                if (dbName != null)
                {
                    mySqlConnectionStringBuilder.Database = dbName;
                }

                mySqlConnectionStringBuilder.DefaultCommandTimeout = (uint)commandTimeout;
                mySqlConnectionStringBuilder.ConnectionTimeout = (uint)connectionTimeout;
                mySqlConnectionStringBuilder.Pooling = true;

                url = mySqlConnectionStringBuilder.ConnectionString;
                break;
            case DatabaseEngine.PG:
                NpgsqlConnectionStringBuilder npgsqlConnectionStringBuilder = new();
                npgsqlConnectionStringBuilder.Host = host;
                if (port != null & port > 0)
                {
                    npgsqlConnectionStringBuilder.Port = (int)port!;
                }

                if (username != null)
                {
                    npgsqlConnectionStringBuilder.Username = username;
                }

                if (password != null)
                {
                    npgsqlConnectionStringBuilder.Password = password;
                }

                if (dbName != null)
                {
                    npgsqlConnectionStringBuilder.Database = dbName;
                }

                npgsqlConnectionStringBuilder.Timeout = connectionTimeout;
                npgsqlConnectionStringBuilder.CommandTimeout = commandTimeout;
                npgsqlConnectionStringBuilder.Pooling = true;

                url = npgsqlConnectionStringBuilder.ConnectionString;
                break;
            default:
                throw new NotSupportedException($"Unsupported database engine: {engine}");
        }

        if (plugins != null)
        {
            url += $"; Plugins={plugins}";
        }

        return url;
    }
}
