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
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using AwsWrapperDataProvider.Plugin.SecretsManager.SecretsManager;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

/// <summary>
/// Unit tests for <see cref="SecretsManagerAuthPlugin"/>'s telemetry wiring —
/// per Req 17 and task 15.
///
/// <para>Covers constructor-level counter creation and the
/// <c>"fetch credentials"</c> nested span lifecycle (success + exception)
/// around the AWS Secrets Manager API call inside <c>UpdateSecrets</c>.</para>
/// </summary>
public class SecretsManagerAuthPluginTelemetryTests
{
    private const string Region = "us-east-2";
    private const int Port = 5432;
    private const string SecretId = "test-secret-id";
    private static readonly string Host = $"fake.host.{Region}.rds.amazonaws.com";

    private readonly Dictionary<string, string> props = new();
    private readonly Mock<IPluginService> mockPluginService = new();
    private readonly Mock<AmazonSecretsManagerClient> mockClient;
    private readonly Mock<ITelemetryFactory> mockFactory = new();
    private readonly Mock<ITelemetryCounter> mockCounter = new();
    private readonly Mock<ITelemetryContext> fetchCredentialsContext = new();
    private readonly HostSpec hostSpec = new(Host, Port, HostRole.Writer, HostAvailability.Available);
    private readonly ADONetDelegate<DbConnection> methodFunc = () => Task.FromResult(new Mock<DbConnection>().Object);

    public SecretsManagerAuthPluginTelemetryTests()
    {
        SecretsManagerAuthPlugin.ClearCache();

        this.mockClient = new Mock<AmazonSecretsManagerClient>(
            Mock.Of<Amazon.Runtime.AWSCredentials>(),
            new AmazonSecretsManagerConfig { RegionEndpoint = Amazon.RegionEndpoint.USEast1 });

        this.props[PropertyDefinition.SecretsManagerSecretId.Name] = SecretId;
        this.props[PropertyDefinition.SecretsManagerRegion.Name] = Region;

        this.mockFactory.Setup(f => f.CreateCounter(It.IsAny<string>())).Returns(this.mockCounter.Object);
        this.mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(this.fetchCredentialsContext.Object);
        this.mockPluginService.Setup(s => s.TelemetryFactory).Returns(this.mockFactory.Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_CreatesFetchCredentialsCounter()
    {
        _ = new SecretsManagerAuthPlugin(this.mockPluginService.Object, this.props, this.mockClient.Object);

        // Req 17.1 — one counter created in constructor with expected name.
        this.mockFactory.Verify(f => f.CreateCounter("secretsManager.fetchCredentials.count"), Times.Once);
        this.mockFactory.Verify(f => f.CreateCounter(It.IsAny<string>()), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task FetchCredentials_OnSuccess_OpensSpanAndIncrementsCounter()
    {
        this.mockClient
            .Setup(c => c.GetSecretValueAsync(It.IsAny<GetSecretValueRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetSecretValueResponse
            {
                SecretString = "{\"username\":\"test-user\",\"password\":\"test-password\"}",
            });

        SecretsManagerAuthPlugin plugin = new(this.mockPluginService.Object, this.props, this.mockClient.Object);

        _ = await plugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc, true);

        // Req 17.2 — one "fetch credentials" Nested span per fetch, success
        // recorded, closed in finally.
        this.mockFactory.Verify(
            f => f.OpenTelemetryContext("fetch credentials", TelemetryTraceLevel.Nested),
            Times.Once);
        this.fetchCredentialsContext.Verify(c => c.SetSuccess(true), Times.Once);
        this.fetchCredentialsContext.Verify(c => c.SetException(It.IsAny<Exception>()), Times.Never);
        this.fetchCredentialsContext.Verify(c => c.CloseContext(), Times.Once);

        // Req 17.1 — counter incremented exactly once (inside the span's try).
        this.mockCounter.Verify(c => c.Inc(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task FetchCredentials_WhenSdkThrows_RecordsExceptionOnSpanAndRethrows()
    {
        InvalidOperationException underlying = new("SM API failure");
        this.mockClient
            .Setup(c => c.GetSecretValueAsync(It.IsAny<GetSecretValueRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(underlying);

        SecretsManagerAuthPlugin plugin = new(this.mockPluginService.Object, this.props, this.mockClient.Object);

        // SecretsManagerUtility.GetRdsSecretFromAwsSecretsManager catches
        // SDK exceptions and rethrows a wrapped `Exception`. Our telemetry
        // catch sees that wrapped exception, not the original SDK one.
        Exception thrown = await Assert.ThrowsAsync<Exception>(
            () => plugin.OpenConnection(this.hostSpec, this.props, true, this.methodFunc, true));

        // Span records the exception the catch block actually sees — the
        // wrapped Exception with the original SDK exception as its
        // InnerException (accessible via `thrown.InnerException`).
        this.mockFactory.Verify(
            f => f.OpenTelemetryContext("fetch credentials", TelemetryTraceLevel.Nested),
            Times.Once);
        this.fetchCredentialsContext.Verify(
            c => c.SetException(It.Is<Exception>(e => e.InnerException == underlying)),
            Times.Once);
        this.fetchCredentialsContext.Verify(c => c.SetSuccess(false), Times.Once);
        this.fetchCredentialsContext.Verify(c => c.SetSuccess(true), Times.Never);
        this.fetchCredentialsContext.Verify(c => c.CloseContext(), Times.Once);

        // Counter fires regardless of outcome — it counts fetch attempts.
        this.mockCounter.Verify(c => c.Inc(), Times.Once);
    }
}
