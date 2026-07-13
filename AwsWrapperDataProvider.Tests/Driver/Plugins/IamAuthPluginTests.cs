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
using AwsWrapperDataProvider.Authentication;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Auth;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using AwsWrapperDataProvider.Plugin.Iam.Iam;
using Microsoft.Extensions.Caching.Memory;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

public class IamAuthPluginTests : IDisposable
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
        PasswordProviderRegistry.Clear();
        AwsCredentialsManager.ResetCustomHandler();

        this.mockPluginService = new Mock<IPluginService>();
        this.mockIamTokenUtility = new Mock<IIamTokenUtility>();
        this.props[PropertyDefinition.Plugins.Name] = "iam";
        this.props[PropertyDefinition.IamDefaultPort.Name] = Port.ToString();
        this.props[PropertyDefinition.User.Name] = User;

        // IamAuthPlugin now creates telemetry instruments in its field
        // initializers using pluginService.TelemetryFactory — wire it to
        // the null factory singleton so all counters/gauges are no-op.
        this.mockPluginService.Setup(s => s.TelemetryFactory).Returns(NullTelemetryFactory.Instance);

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

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithPasswordProviderDialect_RegistersProviderAndRemovesPassword()
    {
        var mockDialect = new Mock<ITargetConnectionDialect>();
        mockDialect.Setup(d => d.SupportsPasswordProvider).Returns(true);
        this.mockPluginService.Setup(s => s.TargetConnectionDialect).Returns(mockDialect.Object);

        _ = await this.iamAuthPlugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc.Object, true);

        // Password must not be in the connection props (kept out of the pool key).
        Assert.False(this.props.ContainsKey(PropertyDefinition.Password.Name));
        // The provider key points at the endpoint cache key.
        Assert.Equal(this.cacheKey, this.props[PasswordProviderRegistry.ProviderKeyPropertyName]);
        // The token was still primed in the shared cache.
        Assert.Equal("generated-token", IamAuthPlugin.IamTokenCache.Get<string>(this.cacheKey));

        // A durable provider was registered and returns the cached token.
        Assert.True(PasswordProviderRegistry.TryGet(this.cacheKey, out var registration));
        Assert.NotNull(registration);
        Assert.Equal("generated-token", await registration!.Provider(CancellationToken.None));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithPasswordProviderDialectAndLoginError_RefreshesCachedToken()
    {
        var mockDialect = new Mock<ITargetConnectionDialect>();
        mockDialect.Setup(d => d.SupportsPasswordProvider).Returns(true);
        this.mockPluginService.Setup(s => s.TargetConnectionDialect).Returns(mockDialect.Object);

        IamAuthPlugin.IamTokenCache.Set(this.cacheKey, "stale-token", TimeSpan.FromDays(999));
        this.methodFunc.SetupSequence(f => f())
            .Throws(new Exception("Error"))
            .Returns(Task.FromResult(new Mock<DbConnection>().Object));
        this.mockPluginService.Setup(s => s.IsLoginException(It.IsAny<Exception>())).Returns(true);

        _ = await this.iamAuthPlugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc.Object, true);

        // The retry refreshed the cache; the registered provider serves the new token.
        Assert.Equal("generated-token", IamAuthPlugin.IamTokenCache.Get<string>(this.cacheKey));
        Assert.True(PasswordProviderRegistry.TryGet(this.cacheKey, out var registration));
        Assert.Equal("generated-token", await registration!.Provider(CancellationToken.None));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithNoHandler_PassesNullCredentials()
    {
        _ = await this.iamAuthPlugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc.Object, true);

        // With no registered handler, the token is generated using the SDK default credentials chain,
        // which the utility receives as null credentials.
        this.mockIamTokenUtility.Verify(
            utility => utility.GenerateAuthenticationTokenAsync(Region, Host, Port, User, null),
            Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithCustomHandler_PassesHandlerCredentials()
    {
        var sentinel = new BasicAWSCredentials("access", "secret");
        AwsCredentialsManager.SetCustomHandler((_, _) => sentinel);

        _ = await this.iamAuthPlugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc.Object, true);

        this.mockIamTokenUtility.Verify(
            utility => utility.GenerateAuthenticationTokenAsync(
                Region, Host, Port, User, It.Is<AWSCredentials>(c => ReferenceEquals(c, sentinel))),
            Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithCachedToken_DoesNotInvokeHandler()
    {
        IamAuthPlugin.IamTokenCache.Set(this.cacheKey, "cached-token", TimeSpan.FromDays(999));

        bool handlerInvoked = false;
        AwsCredentialsManager.SetCustomHandler((_, _) =>
        {
            handlerInvoked = true;
            return new BasicAWSCredentials("access", "secret");
        });

        _ = await this.iamAuthPlugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc.Object, true);

        // The token came from the cache, so the credentials handler must never run.
        Assert.False(handlerInvoked);
        this.mockIamTokenUtility.Verify(
            utility => utility.GenerateAuthenticationTokenAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<AWSCredentials?>()),
            Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task PasswordProvider_Refresh_ReinvokesHandler()
    {
        var mockDialect = new Mock<ITargetConnectionDialect>();
        mockDialect.Setup(d => d.SupportsPasswordProvider).Returns(true);
        this.mockPluginService.Setup(s => s.TargetConnectionDialect).Returns(mockDialect.Object);

        int handlerInvocations = 0;
        AwsCredentialsManager.SetCustomHandler((_, _) =>
        {
            handlerInvocations++;
            return new BasicAWSCredentials("access", "secret");
        });

        _ = await this.iamAuthPlugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc.Object, true);
        Assert.True(PasswordProviderRegistry.TryGet(this.cacheKey, out var registration));

        // Prime a cache miss so the provider must generate a fresh token, which re-resolves credentials
        // through the manager rather than reusing a captured value.
        IamAuthPlugin.ClearCache();
        int invocationsBeforeRefresh = handlerInvocations;

        Assert.Equal("generated-token", await registration!.Provider(CancellationToken.None));
        Assert.True(handlerInvocations > invocationsBeforeRefresh);
    }

    public void Dispose()
    {
        AwsCredentialsManager.ResetCustomHandler();
        GC.SuppressFinalize(this);
    }

    private static string GetCacheKey(string user, string hostname, int port, string region)
    {
        return $"{user}:{hostname}:{port}:{region}";
    }
}
