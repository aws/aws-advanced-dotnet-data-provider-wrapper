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
using Microsoft.Extensions.Caching.Memory;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

/// <summary>
/// Unit tests for <see cref="OktaAuthPlugin"/>'s telemetry wiring.
///
/// <para>Okta auth instruments only a counter (no span and no gauge), so
/// these tests cover constructor counter creation and counter increment on
/// token fetch. Both the cache-miss path and the post-login-exception retry
/// path route through <c>UpdateAuthenticationTokenAsync</c>, where the
/// increment lives — the cache-miss test implicitly verifies both call sites
/// go through the same instrumented code.</para>
/// </summary>
public class OktaAuthPluginTelemetryTests
{
    private sealed class SimpleCredentialsProvider(AWSCredentials credentials) : AWSCredentialsProvider
    {
        private readonly AWSCredentials credentials = credentials;

        public override AWSCredentials GetAWSCredentials() => this.credentials;
    }

    private const string Region = "us-east-2";
    private const int Port = 5432;
    private const string DbUser = "db-user";
    private const string GeneratedToken = "generated-token";
    private static readonly string Host = $"fake.host.{Region}.rds.amazonaws.com";

    private readonly Dictionary<string, string> props = new();
    private readonly Mock<IPluginService> mockPluginService = new();
    private readonly Mock<AWSCredentials> mockCredentials = new();
    private readonly Mock<CredentialsProviderFactory> mockCredentialsProviderFactory = new();
    private readonly Mock<ITokenUtility> mockTokenUtility = new();
    private readonly Mock<ITelemetryFactory> mockFactory = new();
    private readonly Mock<ITelemetryCounter> mockCounter = new();
    private readonly HostSpec hostSpec = new(Host, Port, HostRole.Writer, HostAvailability.Available);
    private readonly ADONetDelegate<DbConnection> methodFunc = () => Task.FromResult(new Mock<DbConnection>().Object);

    public OktaAuthPluginTelemetryTests()
    {
        OktaAuthPlugin.IamTokenCache.Clear();

        this.props[PropertyDefinition.Plugins.Name] = "okta";
        this.props[PropertyDefinition.IamDefaultPort.Name] = Port.ToString();
        this.props[PropertyDefinition.IdpUsername.Name] = "idp-username";
        this.props[PropertyDefinition.IdpPassword.Name] = "idp-password";
        this.props[PropertyDefinition.DbUser.Name] = DbUser;

        this.mockFactory.Setup(f => f.CreateCounter(It.IsAny<string>())).Returns(this.mockCounter.Object);
        this.mockPluginService.Setup(s => s.TelemetryFactory).Returns(this.mockFactory.Object);

        this.mockCredentialsProviderFactory
            .Setup(f => f.GetAwsCredentialsProviderAsync(It.IsAny<string>(), It.IsAny<RegionEndpoint>(), It.IsAny<Dictionary<string, string>>()))
            .ReturnsAsync(new SimpleCredentialsProvider(this.mockCredentials.Object));

        this.mockTokenUtility
            .Setup(t => t.GetCacheKey(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>()))
            .Returns<string, string, int, string>((user, host, port, region) => $"{region}:{host}:{port}:{user}");

        this.mockTokenUtility
            .Setup(t => t.GenerateAuthenticationTokenAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<AWSCredentials>()))
            .ReturnsAsync(GeneratedToken);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_CreatesFetchTokenCounter()
    {
        _ = new OktaAuthPlugin(
            this.mockPluginService.Object,
            this.props,
            this.mockCredentialsProviderFactory.Object,
            this.mockTokenUtility.Object);

        // One counter created in constructor with expected name.
        this.mockFactory.Verify(f => f.CreateCounter("oktaAuth.fetchToken.count"), Times.Once);
        this.mockFactory.Verify(f => f.CreateCounter(It.IsAny<string>()), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task CacheMissFetch_IncrementsFetchTokenCounterOnce()
    {
        OktaAuthPlugin plugin = new(
            this.mockPluginService.Object,
            this.props,
            this.mockCredentialsProviderFactory.Object,
            this.mockTokenUtility.Object);

        _ = await plugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc, true);

        // Counter incremented exactly once per fetch (the cache-miss path
        // fetches once inside UpdateAuthenticationTokenAsync).
        this.mockCounter.Verify(c => c.Inc(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task CacheHit_DoesNotIncrementFetchTokenCounter()
    {
        // Pre-populate the cache for the computed key so ConnectInternal takes
        // the cached branch and no fetch occurs.
        string cacheKey = $"{Region}:{Host}:{Port}:{DbUser}";
        OktaAuthPlugin.IamTokenCache.Set(cacheKey, "cached-token", TimeSpan.FromDays(1));

        OktaAuthPlugin plugin = new(
            this.mockPluginService.Object,
            this.props,
            this.mockCredentialsProviderFactory.Object,
            this.mockTokenUtility.Object);

        _ = await plugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc, true);

        // No fetch happened → counter not incremented.
        this.mockCounter.Verify(c => c.Inc(), Times.Never);
    }
}
