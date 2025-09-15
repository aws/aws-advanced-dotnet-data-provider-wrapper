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

using AwsWrapperDataProvider.Tests.Container.Utils;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class IamConnectivityTests : IntegrationTestBase
{
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public void PgWrapper_WithIamPlugin()
    {
        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, iamUser, null, DefaultDbName);
        connectionString += $";Plugins=iam;IamRegion={iamRegion}";

        using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);

        Console.WriteLine("1. Opening connection via IAM...");
        connection.Open();
        Console.WriteLine("   ✓ Connected successfully");
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public void MySqlClientWrapper_WithIamPlugin()
    {
        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, iamUser, null, DefaultDbName);
        connectionString += $";Plugins=iam;IamRegion={iamRegion}";

        using AwsWrapperConnection<MySql.Data.MySqlClient.MySqlConnection> connection = new(connectionString);

        Console.WriteLine("1. Opening connection via IAM...");
        connection.Open();
        Console.WriteLine("   ✓ Connected successfully");
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public void MySqlConnectorWrapper_WithIamPlugin()
    {
        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(Engine, ClusterEndpoint, Port, iamUser, null, DefaultDbName);
        connectionString += $";Plugins=iam;IamRegion={iamRegion}";

        using AwsWrapperConnection<MySqlConnector.MySqlConnection> connection = new(connectionString);

        Console.WriteLine("1. Opening connection via IAM...");
        connection.Open();
        Console.WriteLine("   ✓ Connected successfully");
    }
}
