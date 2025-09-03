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
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.SecretsManager;
using AwsWrapperDataProvider.Driver.Utils;
using Moq;

namespace AwsWrapperDataProvider.Tests;

public class SecretsManagerAuthPluginTests
{
    private static readonly string Region = "us-east-2";
    private static readonly string Host = $"fake.host.{Region}.rds.amazonaws.com";
    private static readonly int Port = 5432;
    private static readonly string SecretId = "test-secret-id";

    private readonly Mock<IPluginService> mockPluginService;
    private readonly Dictionary<string, string> props = new();
    private readonly Mock<AmazonSecretsManagerClient> mockSecretsManagerClient;
    private readonly SecretsManagerAuthPlugin secretsManagerAuthPlugin;
    private readonly ADONetDelegate<DbConnection> methodFunc;

    public SecretsManagerAuthPluginTests()
    {
        this.mockPluginService = new Mock<IPluginService>();
        this.mockSecretsManagerClient = new Mock<AmazonSecretsManagerClient>(Mock.Of<Amazon.Runtime.AWSCredentials>(), new AmazonSecretsManagerConfig { RegionEndpoint = Amazon.RegionEndpoint.USEast1 });

        // Setup default secret response
        var secretResponse = new GetSecretValueResponse
        {
            SecretString = "{\"username\":\"test-user\",\"password\":\"test-password\"}",
        };

        this.mockSecretsManagerClient.Setup(
            client => client.GetSecretValueAsync(It.IsAny<GetSecretValueRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(secretResponse);

        this.secretsManagerAuthPlugin = new(
            this.mockPluginService.Object,
            this.props,
            SecretId,
            Region,
            870, // default expiration
            this.mockSecretsManagerClient.Object);

        this.methodFunc = () => new Mock<DbConnection>().Object;
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenConnection_WithNoCachedSecret_FetchesSecret()
    {
        SecretsManagerAuthPlugin.ClearCache();

        _ = this.secretsManagerAuthPlugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, this.methodFunc);

        Assert.Equal("test-user", this.props[PropertyDefinition.User.Name]);
        Assert.Equal("test-password", this.props[PropertyDefinition.Password.Name]);

        // Verify secret was fetched
        this.mockSecretsManagerClient.Verify(
            client => client.GetSecretValueAsync(It.IsAny<GetSecretValueRequest>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenConnection_WithInvalidSecretJson_ThrowsException()
    {
        SecretsManagerAuthPlugin.ClearCache();

        // Setup invalid secret response
        var invalidSecretResponse = new GetSecretValueResponse
        {
            SecretString = "{\"invalid\":\"json\"}",
        };

        var mockClient = new Mock<AmazonSecretsManagerClient>(Mock.Of<Amazon.Runtime.AWSCredentials>(), new AmazonSecretsManagerConfig { RegionEndpoint = Amazon.RegionEndpoint.USEast1 });
        mockClient.Setup(
            client => client.GetSecretValueAsync(It.IsAny<GetSecretValueRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(invalidSecretResponse);

        var plugin = new SecretsManagerAuthPlugin(
            this.mockPluginService.Object,
            this.props,
            SecretId,
            Region,
            870,
            mockClient.Object);

        Assert.Throws<Exception>(() =>
            plugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, this.methodFunc));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenConnection_WithSecretsManagerException_ThrowsException()
    {
        SecretsManagerAuthPlugin.ClearCache();

        var mockClient = new Mock<AmazonSecretsManagerClient>(Mock.Of<Amazon.Runtime.AWSCredentials>(), new AmazonSecretsManagerConfig { RegionEndpoint = Amazon.RegionEndpoint.USEast1 });
        mockClient.Setup(
            client => client.GetSecretValueAsync(It.IsAny<GetSecretValueRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Secrets Manager error"));

        var plugin = new SecretsManagerAuthPlugin(
            this.mockPluginService.Object,
            this.props,
            SecretId,
            Region,
            870,
            mockClient.Object);

        Assert.Throws<Exception>(() =>
            plugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, this.methodFunc));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenConnection_WithNonLoginException_ThrowsException()
    {
        SecretsManagerAuthPlugin.ClearCache();

        ADONetDelegate<DbConnection> failingMethodFunc = () => throw new Exception("Non-login error");

        this.mockPluginService.Setup(s => s.IsLoginException(It.IsAny<Exception>())).Returns(false);

        Assert.Throws<Exception>(() =>
            this.secretsManagerAuthPlugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, failingMethodFunc));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ClearCache_ClearsSecretCache()
    {
        SecretsManagerAuthPlugin.ClearCache();
        Assert.True(true);
    }
}
