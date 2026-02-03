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

using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public class LimitlessConnectivityTests : IntegrationTestBase
{
    private static readonly ILogger<LimitlessConnectivityTests> Logger = LoggerUtils.GetLogger<LimitlessConnectivityTests>();
    private readonly AuroraTestUtils auroraTestUtils;

    public LimitlessConnectivityTests()
    {
        this.auroraTestUtils = AuroraTestUtils.GetUtility();
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora-limitless")]
    public void PgWrapper_LimitlessValidConnectionProperties()
    {
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, plugins: "limitless");

        using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);

        Logger.LogInformation("1. Opening connection with limitless plugin...");
        connection.Open();
        Logger.LogInformation("   ✓ Connected successfully");

        var instanceId = this.auroraTestUtils.QueryInstanceId(connection);
        Assert.NotNull(instanceId);
        Assert.NotEmpty(instanceId);
        Logger.LogInformation("   ✓ Instance ID: {InstanceId}", instanceId);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora-limitless")]
    public void PgWrapper_LimitlessAndIamPlugin()
    {
        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, iamUser, null, DefaultDbName, plugins: "iam,limitless");
        connectionString += $";IamRegion={iamRegion}";

        using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);

        Logger.LogInformation("1. Opening connection with limitless and IAM plugins...");
        connection.Open();
        Logger.LogInformation("   ✓ Connected successfully");

        var instanceId = this.auroraTestUtils.QueryInstanceId(connection);
        Assert.NotNull(instanceId);
        Assert.NotEmpty(instanceId);
        Logger.LogInformation("   ✓ Instance ID: {InstanceId}", instanceId);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora-limitless")]
    public void PgWrapper_LimitlessAndAwsSecretsManagerPlugin()
    {
        var secretName = $"TestSecret-{Guid.NewGuid()}";
        _ = this.auroraTestUtils.CreateSecrets(secretName);

        try
        {
            var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, null, null, DefaultDbName, plugins: "awsSecretsManager,limitless");
            connectionString += $";SecretsManagerSecretId={secretName};SecretsManagerRegion={TestEnvironment.Env.Info.Region}";

            using AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString);

            Logger.LogInformation("1. Opening connection with limitless and Secrets Manager plugins...");
            connection.Open();
            Logger.LogInformation("   ✓ Connected successfully");

            var instanceId = this.auroraTestUtils.QueryInstanceId(connection);
            Assert.NotNull(instanceId);
            Assert.NotEmpty(instanceId);
            Logger.LogInformation("   ✓ Instance ID: {InstanceId}", instanceId);
        }
        finally
        {
            this.auroraTestUtils.DeleteSecrets(secretName);
        }
    }
}
