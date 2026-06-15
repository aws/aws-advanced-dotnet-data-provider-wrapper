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

using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins.AuroraConnectionTracker;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.AuroraConnectionTracker;

/// <summary>
/// Unit tests for <see cref="OpenedConnectionTracker"/>'s telemetry wiring.
///
/// <para>Covers:</para>
/// <list type="bullet">
///   <item>Success path: the <c>"invalidate connections"</c>
///     <see cref="TelemetryTraceLevel.Nested"/> span is opened, SetSuccess(true)
///     is recorded, and CloseContext is called.</item>
///   <item>Exception path: SetException + SetSuccess(false) are recorded, the
///     span is still closed, and the underlying exception propagates.</item>
///   <item>Test-harness fallback: when no plugin service is wired, or the
///     plugin service has no telemetry factory configured,
///     <c>InvalidateAllConnections</c> still completes without throwing —
///     the tracker's null-guard falls back to
///     <see cref="NullTelemetryFactory"/>.</item>
/// </list>
/// </summary>
public class OpenedConnectionTrackerTelemetryTests
{
    private const string TestHost = "test-instance.xyz.us-east-1.rds.amazonaws.com";
    private const int TestPort = 5432;

    public OpenedConnectionTrackerTelemetryTests()
    {
        // Each test starts with a clean static tracking map. The production
        // class uses a static dictionary; releasing it here keeps tests
        // isolated from each other's state.
        OpenedConnectionTracker.ReleaseResources();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void InvalidateAllConnections_OpensNestedInvalidateConnectionsSpanAndRecordsSuccess()
    {
        Mock<ITelemetryFactory> mockFactory = new();
        Mock<ITelemetryContext> mockContext = new();
        mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(mockContext.Object);

        Mock<IPluginService> mockPluginService = new();
        mockPluginService.Setup(s => s.TelemetryFactory).Returns(mockFactory.Object);

        OpenedConnectionTracker tracker = new(mockPluginService.Object);
        HostSpec hostSpec = new(TestHost, TestPort, HostRole.Writer, HostAvailability.Available);

        tracker.InvalidateAllConnections(hostSpec);

        // Span name is "invalidate connections" at Nested level, opened
        // exactly once per call.
        mockFactory.Verify(
            f => f.OpenTelemetryContext("invalidate connections", TelemetryTraceLevel.Nested),
            Times.Once);

        // Standard success-path pattern.
        mockContext.Verify(c => c.SetSuccess(true), Times.Once);
        mockContext.Verify(c => c.SetException(It.IsAny<Exception>()), Times.Never);
        mockContext.Verify(c => c.SetSuccess(false), Times.Never);
        mockContext.Verify(c => c.CloseContext(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void InvalidateAllConnections_OnException_RecordsExceptionSetsSuccessFalseAndCloses()
    {
        Mock<ITelemetryFactory> mockFactory = new();
        Mock<ITelemetryContext> mockContext = new();
        mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(mockContext.Object);

        Mock<IPluginService> mockPluginService = new();
        mockPluginService.Setup(s => s.TelemetryFactory).Returns(mockFactory.Object);

        OpenedConnectionTracker tracker = new(mockPluginService.Object);

        // Passing null forces hostSpec.GetHostAndPort() inside the try block to
        // throw — a deterministic way to exercise the catch branch without
        // invasive mock surgery. The public contract is non-null; this test
        // verifies the telemetry wrap records and rethrows rather than
        // silently swallowing unexpected exceptions.
        NullReferenceException thrown = Assert.Throws<NullReferenceException>(
            () => tracker.InvalidateAllConnections(null!));

        // Standard exception-path pattern.
        mockFactory.Verify(
            f => f.OpenTelemetryContext("invalidate connections", TelemetryTraceLevel.Nested),
            Times.Once);
        mockContext.Verify(
            c => c.SetException(It.Is<NullReferenceException>(e => ReferenceEquals(e, thrown))),
            Times.Once);
        mockContext.Verify(c => c.SetSuccess(false), Times.Once);
        mockContext.Verify(c => c.SetSuccess(true), Times.Never);
        mockContext.Verify(c => c.CloseContext(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void InvalidateAllConnections_NoPluginServiceWired_CompletesWithoutThrowing()
    {
        // Test-harness case: no plugin service passed. The tracker's
        // null-guard must fall back to NullTelemetryFactory.Instance so
        // InvalidateAllConnections doesn't NRE on the null pluginService.
        OpenedConnectionTracker tracker = new(pluginService: null);
        HostSpec hostSpec = new(TestHost, TestPort, HostRole.Writer, HostAvailability.Available);

        Exception? caught = Record.Exception(() => tracker.InvalidateAllConnections(hostSpec));
        Assert.Null(caught);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void InvalidateAllConnections_NoTelemetryFactoryOnPluginService_CompletesWithoutThrowing()
    {
        // Related test-harness case: plugin service is wired but its
        // TelemetryFactory property has no mock setup — Moq returns
        // default(ITelemetryFactory) == null. The null-guard must fall back
        // to NullTelemetryFactory.Instance. This is the exact shape the
        // existing OpenedConnectionTrackerTests use, so this test pins the
        // no-telemetry-setup baseline against future regressions.
        Mock<IPluginService> mockPluginService = new();
        OpenedConnectionTracker tracker = new(mockPluginService.Object);
        HostSpec hostSpec = new(TestHost, TestPort, HostRole.Writer, HostAvailability.Available);

        Exception? caught = Record.Exception(() => tracker.InvalidateAllConnections(hostSpec));
        Assert.Null(caught);
    }
}
