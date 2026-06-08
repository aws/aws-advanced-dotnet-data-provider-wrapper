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
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using AwsWrapperDataProvider.Plugin.FederatedAuth.FederatedAuth;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

/// <summary>
/// Unit tests for <see cref="FederatedAuthPlugin"/>'s telemetry wiring.
///
/// <para>Covers constructor-level counter creation and increment semantics on
/// the three code paths that land in <c>UpdateAuthenticationTokenAsync</c>:
/// the initial cache-miss fetch, the cache-hit no-op path, and the
/// login-exception retry-fetch path.</para>
/// </summary>
public class FederatedAuthPluginTelemetryTests
{
    private const string User = "federated-user";
    private const string Region = "us-east-2";
    private const int Port = 5432;
    private const string Password = "idp-password";
    private static readonly string Host = $"fake.host.{Region}.rds.amazonaws.com";

    private readonly Dictionary<string, string> props = new();
    private readonly Mock<IPluginService> mockPluginService = new();
    private readonly Mock<ITokenUtility> mockTokenUtility = new();
    private readonly Mock<CredentialsProviderFactory> mockCredentialsFactory = new();
    private readonly Mock<AWSCredentialsProvider> mockCredentialsProvider = new();
    private readonly HostSpec hostSpec = new(Host, Port, HostRole.Writer, HostAvailability.Available);
    private readonly Mock<ITelemetryFactory> mockFactory = new();
    private readonly Mock<ITelemetryCounter> mockCounter = new();

    public FederatedAuthPluginTelemetryTests()
    {
        FederatedAuthPlugin.ClearCache();

        // Properties required by ConnectInternal. IdpUsername/IdpPassword
        // fall back to User/Password via SamlUtils.CheckIdpCredentialsWithFallback;
        // IamHost/IamRegion short-circuit the host / region derivation so
        // the tests don't depend on RDS hostname parsing.
        this.props[PropertyDefinition.Plugins.Name] = "federatedAuth";
        this.props[PropertyDefinition.User.Name] = User;
        this.props[PropertyDefinition.Password.Name] = Password;
        this.props[PropertyDefinition.DbUser.Name] = User;
        this.props[PropertyDefinition.IamHost.Name] = Host;
        this.props[PropertyDefinition.IamRegion.Name] = Region;
        this.props[PropertyDefinition.IamDefaultPort.Name] = Port.ToString();

        this.mockFactory.Setup(f => f.CreateCounter(It.IsAny<string>())).Returns(this.mockCounter.Object);
        this.mockPluginService.Setup(s => s.TelemetryFactory).Returns(this.mockFactory.Object);

        // Minimal credentials wiring — the mocked token utility ignores
        // the AWSCredentials object, but CredentialsProviderFactory's
        // contract requires a non-null AWSCredentialsProvider whose
        // GetAWSCredentials returns a non-null AWSCredentials.
        this.mockCredentialsProvider
            .Setup(p => p.GetAWSCredentials())
            .Returns(new BasicAWSCredentials("access-key", "secret-key"));
        this.mockCredentialsFactory
            .Setup(f => f.GetAwsCredentialsProviderAsync(It.IsAny<string>(), It.IsAny<RegionEndpoint>(), It.IsAny<Dictionary<string, string>>()))
            .ReturnsAsync(this.mockCredentialsProvider.Object);

        this.mockTokenUtility
            .Setup(u => u.GetCacheKey(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>()))
            .Returns<string, string, int, string>((user, host, port, region) => $"{user}:{host}:{port}:{region}");
        this.mockTokenUtility
            .Setup(u => u.GenerateAuthenticationTokenAsync(Region, Host, Port, User, It.IsAny<AWSCredentials?>()))
            .ReturnsAsync("generated-token");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_CreatesFetchTokenCounter()
    {
        _ = new FederatedAuthPlugin(
            this.mockPluginService.Object,
            this.mockCredentialsFactory.Object,
            this.mockTokenUtility.Object);

        // The counter is created once in the constructor with the exact
        // documented name.
        this.mockFactory.Verify(f => f.CreateCounter("federatedAuth.fetchToken.count"), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task CacheMissFetch_IncrementsFetchTokenCounterOnce()
    {
        Mock<ADONetDelegate<DbConnection>> methodFunc = new();
        methodFunc.Setup(f => f()).Returns(Task.FromResult(new Mock<DbConnection>().Object));

        FederatedAuthPlugin plugin = new(
            this.mockPluginService.Object,
            this.mockCredentialsFactory.Object,
            this.mockTokenUtility.Object);

        _ = await plugin.OpenConnection(this.hostSpec, this.props, true, methodFunc.Object, true);

        // The counter fires exactly once on a cache-miss fetch, because
        // UpdateAuthenticationTokenAsync runs once during the initial
        // connect and increments the counter at its entry.
        this.mockCounter.Verify(c => c.Inc(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task CachedToken_DoesNotIncrementFetchTokenCounter()
    {
        Mock<ADONetDelegate<DbConnection>> methodFunc = new();
        methodFunc.Setup(f => f()).Returns(Task.FromResult(new Mock<DbConnection>().Object));

        FederatedAuthPlugin plugin = new(
            this.mockPluginService.Object,
            this.mockCredentialsFactory.Object,
            this.mockTokenUtility.Object);

        // First call populates the static IamTokenCache (counter = 1).
        _ = await plugin.OpenConnection(this.hostSpec, this.props, true, methodFunc.Object, true);

        // Second call hits the cache, so UpdateAuthenticationTokenAsync
        // is skipped and the counter must not advance.
        _ = await plugin.OpenConnection(this.hostSpec, this.props, true, methodFunc.Object, true);

        // Total increments across both calls = 1 (only the first cache-miss).
        this.mockCounter.Verify(c => c.Inc(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task LoginExceptionRetry_IncrementsFetchTokenCounterTwiceTotal()
    {
        // First call: methodFunc succeeds and fills the cache (counter = 1).
        // Second call: cached token is used, methodFunc throws a login
        // exception, plugin re-fetches the token (counter = 2) and retries
        // methodFunc successfully.
        Mock<ADONetDelegate<DbConnection>> methodFunc = new();
        methodFunc.SetupSequence(f => f())
            .Returns(Task.FromResult(new Mock<DbConnection>().Object))
            .Throws(new Exception("simulated login failure"))
            .Returns(Task.FromResult(new Mock<DbConnection>().Object));

        this.mockPluginService.Setup(s => s.IsLoginException(It.IsAny<Exception>())).Returns(true);

        FederatedAuthPlugin plugin = new(
            this.mockPluginService.Object,
            this.mockCredentialsFactory.Object,
            this.mockTokenUtility.Object);

        _ = await plugin.OpenConnection(this.hostSpec, this.props, true, methodFunc.Object, true);
        _ = await plugin.OpenConnection(this.hostSpec, this.props, true, methodFunc.Object, true);

        // Both the initial fetch and the retry-fetch increment the counter,
        // so the total across both calls is 2.
        this.mockCounter.Verify(c => c.Inc(), Times.Exactly(2));
    }
}
