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
using Amazon.Runtime;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.Iam;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

public class IamAuthPluginTests
{
    private static readonly string User = "iam-user";
    private static readonly string Region = "us-east-2";
    private static readonly string Host = $"fake.host.{Region}.rds.amazonaws.com";
    private static readonly int Port = 5432;

    private readonly IamAuthPlugin iamAuthPlugin;
    private readonly Mock<IPluginService> mockPluginService;
    private readonly Dictionary<string, string> props = [];
    private readonly Mock<IIamTokenUtility> mockIamTokenUtility;
    private readonly HostSpec hostSpec = new(Host, Port, HostRole.Writer, HostAvailability.Available);
    private readonly string iamTokenUtilityGeneratedToken = "generated-token";
    private readonly string cacheKey = GetCacheKey(User, Host, Port, Region);
    private readonly Mock<ADONetDelegate<DbConnection>> methodFunc;

    public IamAuthPluginTests()
    {
        IamAuthPlugin.ClearCache();

        this.mockPluginService = new Mock<IPluginService>();
        this.mockIamTokenUtility = new Mock<IIamTokenUtility>();
        this.props[PropertyDefinition.Plugins.Name] = "iam";
        this.props[PropertyDefinition.IamDefaultPort.Name] = Port.ToString();
        this.props[PropertyDefinition.User.Name] = User;

        this.mockIamTokenUtility.Setup(
            utility => utility.GetCacheKey(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>()))
            .Returns((string user, string hostname, int port, string region) => GetCacheKey(user, hostname, port, region));
        this.mockIamTokenUtility.Setup(
            utility => utility.GenerateAuthenticationTokenAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<AWSCredentials?>()))
            .ReturnsAsync(() => this.iamTokenUtilityGeneratedToken);

        this.iamAuthPlugin = new(this.mockPluginService.Object, this.props, this.mockIamTokenUtility.Object);
        this.methodFunc = new Mock<ADONetDelegate<DbConnection>>();
        this.methodFunc.Setup(f => f()).Returns(Task.FromResult(new Mock<DbConnection>().Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithNoCachedToken_GeneratesToken()
    {
        _ = await this.iamAuthPlugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc.Object, true);
        Assert.Equal("generated-token", this.props[PropertyDefinition.Password.Name]);
        Assert.Equal(1, IamAuthPlugin.IamTokenCache.Count);
        Assert.Equal("generated-token", IamAuthPlugin.IamTokenCache.Get<string>(this.cacheKey));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithCachedToken_UsesCachedToken()
    {
        IamAuthPlugin.IamTokenCache.Set(this.cacheKey, "cached-token", TimeSpan.FromDays(999)); // doesn't expire

        _ = await this.iamAuthPlugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc.Object, true);

        Assert.Equal("cached-token", this.props[PropertyDefinition.Password.Name]);
        Assert.Equal(1, IamAuthPlugin.IamTokenCache.Count);
        Assert.Equal("cached-token", IamAuthPlugin.IamTokenCache.Get<string>(this.cacheKey));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithExpiredCachedToken_GeneratesToken()
    {
        IamAuthPlugin.IamTokenCache.Set(this.cacheKey, "expired-token", TimeSpan.FromMicroseconds(100)); // expires in 100 milliseconds

        // wait 200 milliseconds for token to expire
        await Task.Delay(200, TestContext.Current.CancellationToken);

        _ = await this.iamAuthPlugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc.Object, true);

        Assert.Equal("generated-token", this.props[PropertyDefinition.Password.Name]);
        Assert.Equal(1, IamAuthPlugin.IamTokenCache.Count);
        Assert.Equal("generated-token", IamAuthPlugin.IamTokenCache.Get<string>(this.cacheKey));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithLoginErrorAndCachedToken_GeneratesToken()
    {
        IamAuthPlugin.IamTokenCache.Set(this.cacheKey, "cached-token", TimeSpan.FromDays(999)); // doesn't expire
        this.methodFunc.SetupSequence(f => f())
            .Throws(new Exception("Error"))
            .Returns(Task.FromResult(new Mock<DbConnection>().Object));
        this.mockPluginService.Setup(s => s.IsLoginException(It.IsAny<Exception>())).Returns(true);

        _ = await this.iamAuthPlugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc.Object, true);

        Assert.Equal("generated-token", this.props[PropertyDefinition.Password.Name]);
        Assert.Equal(1, IamAuthPlugin.IamTokenCache.Count);
        Assert.Equal("generated-token", IamAuthPlugin.IamTokenCache.Get<string>(this.cacheKey));
    }

    private static string GetCacheKey(string user, string hostname, int port, string region)
    {
        return $"{user}:{hostname}:{port}:{region}";
    }
}
