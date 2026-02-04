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
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.SecretsManager.SecretsManager;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

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

        this.props[PropertyDefinition.SecretsManagerSecretId.Name] = SecretId;
        this.props[PropertyDefinition.SecretsManagerRegion.Name] = Region;

        this.secretsManagerAuthPlugin = new(
            this.mockPluginService.Object,
            this.props,
            this.mockSecretsManagerClient.Object);

        this.methodFunc = () => Task.FromResult(new Mock<DbConnection>().Object);

        SecretsManagerAuthPlugin.ClearCache();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithNoCachedSecret_FetchesSecret()
    {
        _ = await this.secretsManagerAuthPlugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, this.methodFunc, true);

        Assert.Equal("test-user", this.props[PropertyDefinition.User.Name]);
        Assert.Equal("test-password", this.props[PropertyDefinition.Password.Name]);

        // Verify secret was fetched
        this.mockSecretsManagerClient.Verify(
            client => client.GetSecretValueAsync(It.IsAny<GetSecretValueRequest>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithInvalidSecretJson_ThrowsException()
    {
        // Setup invalid secret response
        var invalidSecretResponse = new GetSecretValueResponse
        {
            SecretString = "{\"invalid\":\"json\"}",
        };

        var mockClient = new Mock<AmazonSecretsManagerClient>(Mock.Of<Amazon.Runtime.AWSCredentials>(),
            new AmazonSecretsManagerConfig { RegionEndpoint = Amazon.RegionEndpoint.USEast1 });
        mockClient.Setup(client =>
                client.GetSecretValueAsync(It.IsAny<GetSecretValueRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(invalidSecretResponse);

        var plugin = new SecretsManagerAuthPlugin(
            this.mockPluginService.Object,
            this.props,
            mockClient.Object);

        await Assert.ThrowsAsync<Exception>(() =>
            plugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, this.methodFunc, true));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithSecretsManagerException_ThrowsException()
    {
        var mockClient = new Mock<AmazonSecretsManagerClient>(Mock.Of<Amazon.Runtime.AWSCredentials>(),
            new AmazonSecretsManagerConfig { RegionEndpoint = Amazon.RegionEndpoint.USEast1 });
        mockClient.Setup(client =>
                client.GetSecretValueAsync(It.IsAny<GetSecretValueRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Secrets Manager error"));

        var plugin = new SecretsManagerAuthPlugin(
            this.mockPluginService.Object,
            this.props,
            mockClient.Object);

        await Assert.ThrowsAsync<Exception>(() =>
            plugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, this.methodFunc, true));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithNonLoginException_ThrowsException()
    {
        ADONetDelegate<DbConnection> failingMethodFunc = () => throw new Exception("Non-login error");

        this.mockPluginService.Setup(s => s.IsLoginException(It.IsAny<Exception>())).Returns(false);

        await Assert.ThrowsAsync<Exception>(() =>
            this.secretsManagerAuthPlugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, failingMethodFunc, true));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithCustomKeys_FetchesCorrectCredentials()
    {
        var customSecretResponse = new GetSecretValueResponse
        {
            SecretString = "{\"db_user\":\"foo\",\"db_pass\":\"bar\"}",
        };

        var mockClient = new Mock<AmazonSecretsManagerClient>(Mock.Of<Amazon.Runtime.AWSCredentials>(),
            new AmazonSecretsManagerConfig { RegionEndpoint = Amazon.RegionEndpoint.USEast1 });
        mockClient.Setup(client => client.GetSecretValueAsync(It.IsAny<GetSecretValueRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(customSecretResponse);

        var customProps = new Dictionary<string, string>
        {
            [PropertyDefinition.SecretsManagerSecretId.Name] = SecretId,
            [PropertyDefinition.SecretsManagerRegion.Name] = Region,
            [PropertyDefinition.SecretsManagerSecretUsernameProperty.Name] = "db_user",
            [PropertyDefinition.SecretsManagerSecretPasswordProperty.Name] = "db_pass",
        };

        var plugin = new SecretsManagerAuthPlugin(
            this.mockPluginService.Object,
            customProps,
            mockClient.Object);

        var testProps = new Dictionary<string, string>();
        _ = await plugin.OpenConnection(
            new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available),
            testProps,
            true,
            this.methodFunc,
            true);

        Assert.Equal("foo", testProps[PropertyDefinition.User.Name]);
        Assert.Equal("bar", testProps[PropertyDefinition.Password.Name]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithMissingCustomKey_ThrowsException()
    {
        var incompleteSecretResponse = new GetSecretValueResponse
        {
            SecretString = "{\"db_user\":\"foo\"}",
        };

        var mockClient = new Mock<AmazonSecretsManagerClient>(Mock.Of<Amazon.Runtime.AWSCredentials>(),
            new AmazonSecretsManagerConfig { RegionEndpoint = Amazon.RegionEndpoint.USEast1 });
        mockClient.Setup(client => client.GetSecretValueAsync(It.IsAny<GetSecretValueRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(incompleteSecretResponse);

        var customProps = new Dictionary<string, string>
        {
            [PropertyDefinition.SecretsManagerSecretId.Name] = SecretId,
            [PropertyDefinition.SecretsManagerRegion.Name] = Region,
            [PropertyDefinition.SecretsManagerSecretUsernameProperty.Name] = "db_user",
            [PropertyDefinition.SecretsManagerSecretPasswordProperty.Name] = "db_pass",
        };

        var plugin = new SecretsManagerAuthPlugin(
            this.mockPluginService.Object,
            customProps,
            mockClient.Object);

        await Assert.ThrowsAsync<Exception>(() =>
            plugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), new Dictionary<string, string>(), true, this.methodFunc, true));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithoutUsernameProperty_FallsBackToDefaultUsername()
    {
        var secretResponse = new GetSecretValueResponse
        {
            SecretString = "{\"username\":\"foo\",\"db_password\":\"bar\"}",
        };

        var mockClient = new Mock<AmazonSecretsManagerClient>(Mock.Of<Amazon.Runtime.AWSCredentials>(),
            new AmazonSecretsManagerConfig { RegionEndpoint = Amazon.RegionEndpoint.USEast1 });
        mockClient.Setup(client => client.GetSecretValueAsync(It.IsAny<GetSecretValueRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(secretResponse);

        var propsWithoutUsernameKey = new Dictionary<string, string>
        {
            [PropertyDefinition.SecretsManagerSecretId.Name] = SecretId,
            [PropertyDefinition.SecretsManagerRegion.Name] = Region,
            [PropertyDefinition.SecretsManagerSecretPasswordProperty.Name] = "db_password",
        };

        var plugin = new SecretsManagerAuthPlugin(
            this.mockPluginService.Object,
            propsWithoutUsernameKey,
            mockClient.Object);

        var testProps = new Dictionary<string, string>();
        _ = await plugin.OpenConnection(
            new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available),
            testProps,
            true,
            this.methodFunc,
            true);

        Assert.Equal("foo", testProps[PropertyDefinition.User.Name]);
        Assert.Equal("bar", testProps[PropertyDefinition.Password.Name]);
    }
}
