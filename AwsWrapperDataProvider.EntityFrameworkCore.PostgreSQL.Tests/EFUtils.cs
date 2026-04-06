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

using System.Text.Json;
using Npgsql;

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL.Tests;

public class EFUtils
{
    public static string GetNpgsqlConnectionString()
    {
        string infoJson = Environment.GetEnvironmentVariable("TEST_ENV_INFO_JSON") ?? throw new Exception("Environment variable TEST_ENV_INFO_JSON is required.");
        using var doc = JsonDocument.Parse(infoJson);
        var root = doc.RootElement;
        var deployment = root.GetProperty("request").GetProperty("deployment").GetString();
        string? host;
        int? port;
        if (deployment == "RDS_MULTI_AZ_INSTANCE")
        {
            host = root.GetProperty("databaseInfo").GetProperty("instances")[0].GetProperty("host").GetString();
            port = root.GetProperty("databaseInfo").GetProperty("instances")[0].GetProperty("port").GetInt32();
        }
        else
        {
            host = root.GetProperty("databaseInfo").GetProperty("clusterEndpoint").GetString() ?? throw new Exception("Could not get cluster endpoint from TEST_ENV_INFO_JSON");
            port = root.GetProperty("databaseInfo").GetProperty("clusterEndpointPort").GetInt32();
        }

        var user = root.GetProperty("databaseInfo").GetProperty("username").GetString() ?? throw new Exception("Could not get user from TEST_ENV_INFO_JSON");
        var password = root.GetProperty("databaseInfo").GetProperty("password").GetString() ?? throw new Exception("Could not get password from TEST_ENV_INFO_JSON");
        var dbName = root.GetProperty("databaseInfo").GetProperty("defaultDbName").GetString() ?? throw new Exception("Could not get defaultDbName from TEST_ENV_INFO_JSON");

        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = host,
            Port = port ?? 5432,
            Username = user,
            Password = password,
            Database = dbName,
            CommandTimeout = 30,
            Timeout = 30,
        };

        return builder.ConnectionString;
    }
}
