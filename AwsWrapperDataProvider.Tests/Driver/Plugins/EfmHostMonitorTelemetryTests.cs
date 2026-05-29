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

using System.Reflection;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins.Efm;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

/// <summary>
/// Unit tests for the EFM <see cref="HostMonitor"/> telemetry.
///
/// <para>Covers two of the three test areas:</para>
/// <list type="bullet">
///   <item>Constructor creates 3 telemetry instruments (2 counters + 1 gauge)
///     with the expected names, including the node id baked into
///     <c>efm.nodeUnhealthy.count.&lt;nodeId&gt;</c>.</item>
///   <item><c>HostMonitor.Run</c> opens the <c>"monitoring thread"</c>
///     <see cref="TelemetryTraceLevel.TopLevel"/> span at thread start with a
///     <c>url</c> attribute, and closes it with <c>SetSuccess(true)</c> on
///     normal cancellation-driven exit.</item>
/// </list>
///
/// <para>The third test area — verifying the <c>"connection status check"</c>
/// <see cref="TelemetryTraceLevel.Nested"/> span in the private
/// <c>CheckConnectionStatusAsync</c> method — is deferred. It depends on a
/// private-method testing-access decision; see
/// <c>.kiro/specs/wrapper-telemetry/deferred-decisions.md</c> for details.</para>
///
/// <para>Access-to-private-state note: <c>HostMonitor</c>'s constructor spawns
/// <c>Run</c> as a background <see cref="Task"/>, and there is no public
/// Stop/Dispose API. The tests use reflection to reach the
/// <c>cancellationTokenSource</c> / <c>runTask</c> / <c>newContextRunTask</c>
/// fields so the tests can cancel cleanly and await shutdown. This is local
/// to the test file and does not affect production code.</para>
/// </summary>
public class EfmHostMonitorTelemetryTests
{
    private const string TestHost = "test-host.example.com";
    private const int TestPort = 5432;
    private const string TestNodeId = "test-node-id";

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Constructor_CreatesThreeTelemetryInstruments()
    {
        Mock<ITelemetryFactory> mockFactory = new();
        Mock<ITelemetryCounter> mockCounter = new();
        Mock<ITelemetryGauge> mockGauge = new();
        mockFactory.Setup(f => f.CreateCounter(It.IsAny<string>())).Returns(mockCounter.Object);
        mockFactory.Setup(f => f.CreateGauge(It.IsAny<string>(), It.IsAny<Func<long>>())).Returns(mockGauge.Object);
        mockFactory.Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(Mock.Of<ITelemetryContext>());

        Mock<IPluginService> mockPluginService = new();
        mockPluginService.Setup(s => s.TelemetryFactory).Returns(mockFactory.Object);

        HostSpec hostSpec = new(TestHost, TestPort, TestNodeId, HostRole.Writer, HostAvailability.Available);

        HostMonitor monitor = new(mockPluginService.Object, hostSpec, new Dictionary<string, string>(), 1000, 1000, 3);
        try
        {
            // Three instruments, each created exactly once in the
            // constructor with the expected names. The nodeUnhealthy counter
            // and activeContexts queue size gauge both use the HostSpec's
            // HostId so concurrent monitors don't collide on instrument name.
            mockFactory.Verify(f => f.CreateCounter("efm.connections.aborted"), Times.Once);
            mockFactory.Verify(f => f.CreateCounter($"efm.nodeUnhealthy.count.{TestNodeId}"), Times.Once);
            mockFactory.Verify(f => f.CreateGauge($"efm.activeContexts.queue.size.{TestNodeId}", It.IsAny<Func<long>>()), Times.Once);
        }
        finally
        {
            await StopMonitorAsync(monitor);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Constructor_WhenHostIdIsNull_FallsBackToHostForNodeIdInInstrumentNames()
    {
        Mock<ITelemetryFactory> mockFactory = new();
        Mock<ITelemetryCounter> mockCounter = new();
        Mock<ITelemetryGauge> mockGauge = new();
        mockFactory.Setup(f => f.CreateCounter(It.IsAny<string>())).Returns(mockCounter.Object);
        mockFactory.Setup(f => f.CreateGauge(It.IsAny<string>(), It.IsAny<Func<long>>())).Returns(mockGauge.Object);
        mockFactory.Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(Mock.Of<ITelemetryContext>());

        Mock<IPluginService> mockPluginService = new();
        mockPluginService.Setup(s => s.TelemetryFactory).Returns(mockFactory.Object);

        // HostId is null — counter name should use Host instead.
        HostSpec hostSpec = new(TestHost, TestPort, null, HostRole.Writer, HostAvailability.Available);

        HostMonitor monitor = new(mockPluginService.Object, hostSpec, new Dictionary<string, string>(), 1000, 1000, 3);
        try
        {
            mockFactory.Verify(f => f.CreateCounter($"efm.nodeUnhealthy.count.{TestHost}"), Times.Once);
            mockFactory.Verify(f => f.CreateGauge($"efm.activeContexts.queue.size.{TestHost}", It.IsAny<Func<long>>()), Times.Once);
        }
        finally
        {
            await StopMonitorAsync(monitor);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Run_OpensMonitoringThreadTopLevelSpanWithUrlAttribute()
    {
        Mock<ITelemetryFactory> mockFactory = new();
        Mock<ITelemetryContext> monitoringThreadContext = new();
        Mock<ITelemetryContext> otherContext = new();
        Mock<ITelemetryCounter> mockCounter = new();
        Mock<ITelemetryGauge> mockGauge = new();

        mockFactory.Setup(f => f.CreateCounter(It.IsAny<string>())).Returns(mockCounter.Object);
        mockFactory.Setup(f => f.CreateGauge(It.IsAny<string>(), It.IsAny<Func<long>>())).Returns(mockGauge.Object);

        // Route the "monitoring thread" span to a context we can assert on;
        // route everything else (if any nested contexts get opened during
        // the brief background run) to a throwaway mock so assertions on
        // `monitoringThreadContext` aren't polluted.
        mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns<string, TelemetryTraceLevel>((name, _) =>
                name == "monitoring thread" ? monitoringThreadContext.Object : otherContext.Object);

        Mock<IPluginService> mockPluginService = new();
        mockPluginService.Setup(s => s.TelemetryFactory).Returns(mockFactory.Object);

        HostSpec hostSpec = new(TestHost, TestPort, TestNodeId, HostRole.Writer, HostAvailability.Available);

        HostMonitor monitor = new(mockPluginService.Object, hostSpec, new Dictionary<string, string>(), 1000, 1000, 3);

        // The background Run task starts immediately in the constructor but
        // we need to give it a moment to open the span before we cancel it.
        // 50 ms is generous for a span-open + set-attribute call (both are
        // synchronous in OtlpTelemetryFactory + NullTelemetryFactory; here
        // we're using Moq mocks which are even faster).
        await Task.Delay(50, TestContext.Current.CancellationToken);

        await StopMonitorAsync(monitor);

        // Exactly one "monitoring thread" span per HostMonitor, at TopLevel.
        mockFactory.Verify(
            f => f.OpenTelemetryContext("monitoring thread", TelemetryTraceLevel.TopLevel),
            Times.Once);

        // url attribute carries the monitored host's host:port identifier.
        monitoringThreadContext.Verify(
            c => c.SetAttribute("url", hostSpec.GetHostAndPort()),
            Times.Once);

        // Cancellation via the CancellationTokenSource is the normal exit
        // path — Run records it as success and closes the context.
        monitoringThreadContext.Verify(c => c.SetSuccess(true), Times.AtLeastOnce);
        monitoringThreadContext.Verify(c => c.SetException(It.IsAny<Exception>()), Times.Never);
        monitoringThreadContext.Verify(c => c.CloseContext(), Times.Once);
    }

    /// <summary>
    /// Cancels the monitor's internal cancellation-token source via reflection
    /// and awaits both background tasks (Run and NewContextRun) so the test
    /// doesn't leak a running monitor.
    /// </summary>
    private static async Task StopMonitorAsync(HostMonitor monitor)
    {
        Type monitorType = typeof(HostMonitor);
        BindingFlags privateInstance = BindingFlags.NonPublic | BindingFlags.Instance;

        if (monitorType.GetField("cancellationTokenSource", privateInstance)?.GetValue(monitor)
                is CancellationTokenSource cts)
        {
            if (!cts.IsCancellationRequested)
            {
                cts.Cancel();
            }
        }

        foreach (string taskFieldName in new[] { "runTask", "newContextRunTask" })
        {
            if (monitorType.GetField(taskFieldName, privateInstance)?.GetValue(monitor) is Task task)
            {
                try
                {
                    await task;
                }
                catch (OperationCanceledException)
                {
                    // Expected on cancellation.
                }
                catch (Exception)
                {
                    // Other exceptions from the background task are not the
                    // subject of these tests; swallow so teardown is robust.
                }
            }
        }
    }
}
