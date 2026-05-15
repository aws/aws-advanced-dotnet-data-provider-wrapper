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
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using AwsWrapperDataProvider.Plugin.Iam.Iam;
using Microsoft.Extensions.Caching.Memory;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

/// <summary>
/// Unit tests for <see cref="IamAuthPlugin"/>'s telemetry wiring — per Req 16
/// and task 14.
///
/// <para>Covers constructor-level instrument creation (counter + gauge), the
/// gauge callback semantics, and the <c>"fetch IAM token"</c> span lifecycle
/// (success + exception) on the cache-miss path.</para>
///
/// <para>The retry-fetch-path test (that span + counter fire a second time
/// when the cached token returns a login exception on the first
/// <c>methodFunc()</c> call) is deferred — see
/// <c>.kiro/specs/wrapper-telemetry/deferred-decisions.md</c>.</para>
/// </summary>
public class IamAuthPluginTelemetryTests
{
    private const string User = "iam-user";
    private const string Region = "us-east-2";
    private const int Port = 5432;
    private static readonly string Host = $"fake.host.{Region}.rds.amazonaws.com";

    private readonly Dictionary<string, string> props = new();
    private readonly Mock<IPluginService> mockPluginService = new();
    private readonly Mock<IIamTokenUtility> mockIamTokenUtility = new();
    private readonly HostSpec hostSpec = new(Host, Port, HostRole.Writer, HostAvailability.Available);
    private readonly Mock<ITelemetryFactory> mockFactory = new();
    private readonly Mock<ITelemetryCounter> mockCounter = new();
    private readonly Mock<ITelemetryGauge> mockGauge = new();
    private readonly Mock<ITelemetryContext> fetchTokenContext = new();
    private Func<long>? capturedGaugeCallback;

    public IamAuthPluginTelemetryTests()
    {
        IamAuthPlugin.ClearCache();

        this.props[PropertyDefinition.Plugins.Name] = "iam";
        this.props[PropertyDefinition.IamDefaultPort.Name] = Port.ToString();
        this.props[PropertyDefinition.User.Name] = User;

        this.mockFactory.Setup(f => f.CreateCounter(It.IsAny<string>())).Returns(this.mockCounter.Object);
        this.mockFactory
            .Setup(f => f.CreateGauge(It.IsAny<string>(), It.IsAny<Func<long>>()))
            .Callback<string, Func<long>>((_, cb) => this.capturedGaugeCallback = cb)
            .Returns(this.mockGauge.Object);
        this.mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(this.fetchTokenContext.Object);

        this.mockPluginService.Setup(s => s.TelemetryFactory).Returns(this.mockFactory.Object);

        this.mockIamTokenUtility
            .Setup(u => u.GetCacheKey(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>()))
            .Returns<string, string, int, string>((user, host, port, region) => $"{region}:{host}:{port}:{user}");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_CreatesFetchTokenCounterAndTokenCacheSizeGauge()
    {
        _ = new IamAuthPlugin(this.mockPluginService.Object, this.props, this.mockIamTokenUtility.Object);

        // Req 16.1 / 16.2 — one counter + one gauge created in constructor,
        // with the expected names.
        this.mockFactory.Verify(f => f.CreateCounter("iam.fetchToken.count"), Times.Once);
        this.mockFactory.Verify(f => f.CreateGauge("iam.tokenCache.size", It.IsAny<Func<long>>()), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GaugeCallback_ReturnsLiveCountOfIamTokenCache()
    {
        _ = new IamAuthPlugin(this.mockPluginService.Object, this.props, this.mockIamTokenUtility.Object);

        Assert.NotNull(this.capturedGaugeCallback);

        // Empty cache → 0.
        Assert.Equal(0, this.capturedGaugeCallback!());

        // After inserting 2 entries, the callback reflects the live count.
        IamAuthPlugin.IamTokenCache.Set("k1", "t1", TimeSpan.FromDays(1));
        IamAuthPlugin.IamTokenCache.Set("k2", "t2", TimeSpan.FromDays(1));
        Assert.Equal(2, this.capturedGaugeCallback!());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task CacheMissFetch_OpensFetchIamTokenSpanAndIncrementsCounter()
    {
        const string generatedToken = "generated-token";
        this.mockIamTokenUtility
            .Setup(u => u.GenerateAuthenticationTokenAsync(Region, Host, Port, User))
            .ReturnsAsync(generatedToken);

        Mock<ADONetDelegate<DbConnection>> methodFunc = new();
        methodFunc.Setup(f => f()).Returns(Task.FromResult(new Mock<DbConnection>().Object));

        IamAuthPlugin plugin = new(this.mockPluginService.Object, this.props, this.mockIamTokenUtility.Object);

        _ = await plugin.OpenConnection(this.hostSpec, this.props, true, methodFunc.Object, true);

        // Req 16.3 — one "fetch IAM token" Nested span opened per fetch,
        // success recorded, closed in finally.
        this.mockFactory.Verify(
            f => f.OpenTelemetryContext("fetch IAM token", TelemetryTraceLevel.Nested),
            Times.Once);
        this.fetchTokenContext.Verify(c => c.SetSuccess(true), Times.Once);
        this.fetchTokenContext.Verify(c => c.SetException(It.IsAny<Exception>()), Times.Never);
        this.fetchTokenContext.Verify(c => c.CloseContext(), Times.Once);

        // Req 16.1 — counter incremented exactly once (inside the span's
        // try so counter/span stay paired).
        this.mockCounter.Verify(c => c.Inc(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task CacheMissFetch_WhenTokenApiThrows_RecordsExceptionOnSpanAndRethrowsWrapped()
    {
        InvalidOperationException underlying = new("AWS STS failure");
        this.mockIamTokenUtility
            .Setup(u => u.GenerateAuthenticationTokenAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>()))
            .ThrowsAsync(underlying);

        Mock<ADONetDelegate<DbConnection>> methodFunc = new();
        methodFunc.Setup(f => f()).Returns(Task.FromResult(new Mock<DbConnection>().Object));

        IamAuthPlugin plugin = new(this.mockPluginService.Object, this.props, this.mockIamTokenUtility.Object);

        // OpenConnection should rethrow the wrapped exception; its inner
        // exception is the original exception from the token API.
        Exception thrown = await Assert.ThrowsAsync<Exception>(
            () => plugin.OpenConnection(this.hostSpec, this.props, true, methodFunc.Object, true));
        Assert.Same(underlying, thrown.InnerException);

        // Span records the underlying exception for root-cause visibility,
        // sets success=false, and closes.
        this.mockFactory.Verify(
            f => f.OpenTelemetryContext("fetch IAM token", TelemetryTraceLevel.Nested),
            Times.Once);
        this.fetchTokenContext.Verify(c => c.SetException(underlying), Times.Once);
        this.fetchTokenContext.Verify(c => c.SetSuccess(false), Times.Once);
        this.fetchTokenContext.Verify(c => c.SetSuccess(true), Times.Never);
        this.fetchTokenContext.Verify(c => c.CloseContext(), Times.Once);

        // Counter fires regardless of outcome — it counts fetch attempts.
        this.mockCounter.Verify(c => c.Inc(), Times.Once);

        // The inner methodFunc must never be called when token fetch fails.
        methodFunc.Verify(f => f(), Times.Never);
    }
}
