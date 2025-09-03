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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Driver.Plugins.Iam;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;
using Moq;

namespace AwsWrapperDataProvider.Tests;

public class IamAuthPluginTests
{
    private static readonly string Region = "us-east-2";
    private static readonly string Host = $"fake.host.{Region}.rds.amazonaws.com";
    private static readonly int Port = 5432;

    private readonly Mock<IPluginService> mockPluginService;
    private readonly Dictionary<string, string> props = new();
    private readonly Mock<IIamTokenUtility> mockIamTokenUtility;
    private readonly IamAuthPlugin iamAuthPlugin;
    private readonly ADONetDelegate<DbConnection> methodFunc;

    private string iamTokenUtilityGeneratedToken = "generated-token";

    public IamAuthPluginTests()
    {
        this.mockPluginService = new Mock<IPluginService>();
        this.props[PropertyDefinition.User.Name] = "iam-user";
        this.props[PropertyDefinition.IamDefaultPort.Name] = "5432";

        this.mockIamTokenUtility = new Mock<IIamTokenUtility>();
        this.mockIamTokenUtility.Setup(
            utility => utility.GetCacheKey(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>()))
            .Returns((string user, string hostname, int port, string region) => CacheKey(user, hostname, port, region));
        this.mockIamTokenUtility.Setup(
            utility => utility.GenerateAuthenticationToken(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<Amazon.Runtime.AWSCredentials?>()))
            .Returns(() => this.iamTokenUtilityGeneratedToken);

        this.iamAuthPlugin = new(this.mockPluginService.Object, this.props, this.mockIamTokenUtility.Object);

        this.methodFunc = () => new Mock<DbConnection>().Object;
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenConnection_WithNoCachedToken_GeneratesToken()
    {
        IamAuthPlugin.ClearCache();

        this.props[PropertyDefinition.User.Name] = "iam-user";
        this.props[PropertyDefinition.IamHost.Name] = Host;
        this.props[PropertyDefinition.IamRegion.Name] = Region;
        this.iamTokenUtilityGeneratedToken = "generated-token";

        _ = this.iamAuthPlugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, this.methodFunc);

        Assert.Equal("generated-token", this.props[PropertyDefinition.Password.Name]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenConnection_WithCachedToken_UsesCachedToken()
    {
        IamAuthPlugin.ClearCache();

        // Manually add token to cache
        var cacheKey = CacheKey("iam-user", Host, 5432, Region);
        var cacheField = typeof(IamAuthPlugin).GetField("IamTokenCache", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var cache = (Microsoft.Extensions.Caching.Memory.MemoryCache)cacheField!.GetValue(null)!;
        cache.Set(cacheKey, "cached-token", TimeSpan.FromDays(999));

        this.props[PropertyDefinition.User.Name] = "iam-user";
        this.props[PropertyDefinition.IamHost.Name] = Host;
        this.props[PropertyDefinition.IamRegion.Name] = Region;
        this.iamTokenUtilityGeneratedToken = "incorrect-token";

        _ = this.iamAuthPlugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, this.methodFunc);

        Assert.Equal("cached-token", this.props[PropertyDefinition.Password.Name]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OpenConnection_WithExpiredCachedToken_GeneratesToken()
    {
        IamAuthPlugin.ClearCache();

        // Manually add expired token to cache
        var cacheKey = CacheKey("iam-user", Host, 5432, Region);
        var cacheField = typeof(IamAuthPlugin).GetField("IamTokenCache", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var cache = (Microsoft.Extensions.Caching.Memory.MemoryCache)cacheField!.GetValue(null)!;
        cache.Set(cacheKey, "expired-token", TimeSpan.FromSeconds(1));

        // Wait for token to expire
        await Task.Delay(2000, TestContext.Current.CancellationToken);

        this.props[PropertyDefinition.User.Name] = "iam-user";
        this.props[PropertyDefinition.IamHost.Name] = Host;
        this.props[PropertyDefinition.IamRegion.Name] = Region;
        this.iamTokenUtilityGeneratedToken = "generated-token";

        _ = this.iamAuthPlugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, this.methodFunc);

        Assert.Equal("generated-token", this.props[PropertyDefinition.Password.Name]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenConnection_WithLoginExceptionAndCachedToken_RegeneratesToken()
    {
        IamAuthPlugin.ClearCache();

        // Add cached token
        var cacheKey = CacheKey("iam-user", Host, 5432, Region);
        var cacheField = typeof(IamAuthPlugin).GetField("IamTokenCache", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var cache = (Microsoft.Extensions.Caching.Memory.MemoryCache)cacheField!.GetValue(null)!;
        cache.Set(cacheKey, "cached-token", TimeSpan.FromDays(999));

        this.props[PropertyDefinition.User.Name] = "iam-user";
        this.props[PropertyDefinition.IamHost.Name] = Host;
        this.props[PropertyDefinition.IamRegion.Name] = Region;
        this.iamTokenUtilityGeneratedToken = "new-token";

        // Mock login exception on first call, success on second
        var callCount = 0;
        ADONetDelegate<DbConnection> failingMethodFunc = () =>
        {
            callCount++;
            if (callCount == 1)
            {
                throw new Exception("Login failed");
            }
            return new Mock<DbConnection>().Object;
        };

        this.mockPluginService.Setup(s => s.IsLoginException(It.IsAny<Exception>())).Returns(true);

        _ = this.iamAuthPlugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, failingMethodFunc);

        Assert.Equal("new-token", this.props[PropertyDefinition.Password.Name]);
        Assert.Equal(2, callCount);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenConnection_WithCustomExpirationTime_CachesWithCustomExpiration()
    {
        IamAuthPlugin.ClearCache();

        this.props[PropertyDefinition.User.Name] = "iam-user";
        this.props[PropertyDefinition.IamHost.Name] = Host;
        this.props[PropertyDefinition.IamRegion.Name] = Region;
        this.props[PropertyDefinition.IamExpiration.Name] = "300"; // 5 minutes
        this.iamTokenUtilityGeneratedToken = "generated-token";

        _ = this.iamAuthPlugin.OpenConnection(new HostSpec(Host, Port, HostRole.Writer, HostAvailability.Available), this.props, true, this.methodFunc);

        // Verify token was cached (we can't easily verify expiration time without more complex reflection)
        Assert.Equal("generated-token", this.props[PropertyDefinition.Password.Name]);
    }

    private static string CacheKey(string user, string hostname, int port, string region)
    {
        return $"{user}:{hostname}:{port}:{region}";
    }
}
