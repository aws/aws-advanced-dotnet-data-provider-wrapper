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
using Amazon;
using Amazon.Runtime;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.FederatedAuth;
using AwsWrapperDataProvider.Driver.Plugins.Iam;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

public class OktaAuthPluginTests
{
    private class SimpleCredentialsProvider(AWSCredentials credentials) : AWSCredentialsProvider
    {
        private readonly AWSCredentials credentials = credentials;

        public override AWSCredentials GetAWSCredentials()
        {
            return this.credentials;
        }
    }

    private static readonly string Region = "us-east-2";
    private static readonly string Host = $"fake.host.{Region}.rds.amazonaws.com";
    private static readonly int Port = 5432;

    private readonly Mock<IPluginService> mockPluginService;
    private readonly Dictionary<string, string> props = [];
    private readonly Mock<AWSCredentials> mockCredentials;
    private readonly Mock<CredentialsProviderFactory> mockCredentialsProviderFactory;
    private readonly Mock<IIamTokenUtility> mockIamTokenUtility;
    private readonly OktaAuthPlugin oktaAuthPlugin;
    private readonly ADONetDelegate<DbConnection> methodFunc;

    private string iamTokenUtilityGeneratedToken = "generated-token";

    public OktaAuthPluginTests()
    {
        OktaAuthPlugin.IamTokenCache.Clear();

        this.mockPluginService = new Mock<IPluginService>();
        this.props[PropertyDefinition.Plugins.Name] = "okta";
        this.props[PropertyDefinition.IamDefaultPort.Name] = Port.ToString();
        this.props[PropertyDefinition.IdpUsername.Name] = "idp-username";
        this.props[PropertyDefinition.IdpPassword.Name] = "idp-password";

        this.mockCredentials = new Mock<AWSCredentials>();
        this.mockCredentialsProviderFactory = new Mock<CredentialsProviderFactory>();
        this.mockCredentialsProviderFactory.Setup(
            factory => factory.GetAwsCredentialsProviderAsync(It.IsAny<string>(), It.IsAny<RegionEndpoint>(), It.IsAny<Dictionary<string, string>>()))
            .ReturnsAsync(new SimpleCredentialsProvider(this.mockCredentials.Object));

        this.mockIamTokenUtility = new Mock<IIamTokenUtility>();
        this.mockIamTokenUtility.Setup(
            utility => utility.GetCacheKey(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>()))
            .Returns((string user, string hostname, int port, string region) => CacheKey(user, hostname, port, region));
        this.mockIamTokenUtility.Setup(
            utility => utility.GenerateAuthenticationToken(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<AWSCredentials?>()))
            .ReturnsAsync(() => this.iamTokenUtilityGeneratedToken);

        this.oktaAuthPlugin = new(this.mockPluginService.Object, this.props, this.mockCredentialsProviderFactory.Object, this.mockIamTokenUtility.Object);

        this.methodFunc = () => Task.FromResult(new Mock<DbConnection>().Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithNoCachedToken_GeneratesToken()
    {
        this.props[PropertyDefinition.DbUser.Name] = "db-user";
        this.iamTokenUtilityGeneratedToken = "generated-token";

        _ = await this.oktaAuthPlugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, this.methodFunc, true);
        Assert.Equal("db-user", this.props[PropertyDefinition.User.Name]);
        Assert.Equal("generated-token", this.props[PropertyDefinition.Password.Name]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithCachedToken_UsesCachedToken()
    {
        OktaAuthPlugin.IamTokenCache.Set(CacheKey("db-user", Host, Port, Region), "cached-token", TimeSpan.FromDays(999)); // doesn't expire

        this.props[PropertyDefinition.DbUser.Name] = "db-user";
        this.iamTokenUtilityGeneratedToken = "incorrect-token";

        _ = await this.oktaAuthPlugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, this.methodFunc, true);

        Assert.Equal("db-user", this.props[PropertyDefinition.User.Name]);
        Assert.Equal("cached-token", this.props[PropertyDefinition.Password.Name]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithExpiredCachedToken_GeneratesToken()
    {
        OktaAuthPlugin.IamTokenCache.Set(CacheKey("db-user", Host, Port, Region), "expired-token", TimeSpan.FromSeconds(1)); // expires in 1 sec

        // wait 2 seconds for token to expire
        await Task.Delay(2000, TestContext.Current.CancellationToken);

        this.props[PropertyDefinition.DbUser.Name] = "db-user";
        this.iamTokenUtilityGeneratedToken = "generated-token";

        _ = await this.oktaAuthPlugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, this.methodFunc, true);

        Assert.Equal("db-user", this.props[PropertyDefinition.User.Name]);
        Assert.Equal("generated-token", this.props[PropertyDefinition.Password.Name]);
    }

    private static string CacheKey(string user, string hostname, int port, string region)
    {
        return $"{user}:{hostname}:{port}:{region}";
    }
}
