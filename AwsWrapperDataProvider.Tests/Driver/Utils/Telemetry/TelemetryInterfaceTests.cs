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

using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Utils.Telemetry;

/// <summary>
/// Contract tests for the telemetry interfaces. These tests verify that the
/// interfaces compile, can be mocked with Moq, and expose the expected
/// members of the public telemetry surface. Implementation behaviour is
/// covered by the per-implementation test classes.
/// </summary>
public class TelemetryInterfaceTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void TelemetryTraceLevel_DefinesExactlyFourValues()
    {
        // Enum with values ForceTopLevel, TopLevel, Nested, NoTrace.
        string[] values = Enum.GetNames<TelemetryTraceLevel>();
        Assert.Equal(4, values.Length);
        Assert.Contains(nameof(TelemetryTraceLevel.ForceTopLevel), values);
        Assert.Contains(nameof(TelemetryTraceLevel.TopLevel), values);
        Assert.Contains(nameof(TelemetryTraceLevel.Nested), values);
        Assert.Contains(nameof(TelemetryTraceLevel.NoTrace), values);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ITelemetryContext_MockRecordsAllInteractions()
    {
        // SetSuccess, SetAttribute, SetException, GetName, CloseContext.
        Mock<ITelemetryContext> mock = new();
        mock.Setup(c => c.GetName()).Returns("span-name");

        ITelemetryContext context = mock.Object;
        context.SetSuccess(true);
        context.SetAttribute("db.system", "mysql");
        context.SetException(new InvalidOperationException("boom"));
        string name = context.GetName();
        context.CloseContext();

        Assert.Equal("span-name", name);
        mock.Verify(c => c.SetSuccess(true), Times.Once);
        mock.Verify(c => c.SetAttribute("db.system", "mysql"), Times.Once);
        mock.Verify(c => c.SetException(It.IsAny<InvalidOperationException>()), Times.Once);
        mock.Verify(c => c.GetName(), Times.Once);
        mock.Verify(c => c.CloseContext(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ITelemetryCounter_MockRecordsAddAndInc()
    {
        Mock<ITelemetryCounter> mock = new();
        ITelemetryCounter counter = mock.Object;

        counter.Add(5);
        counter.Inc();

        mock.Verify(c => c.Add(5), Times.Once);
        mock.Verify(c => c.Inc(), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ITelemetryGauge_IsMockableMarkerInterface()
    {
        // Gauge is a marker interface; nothing to mock on the instance itself.
        Mock<ITelemetryGauge> mock = new();
        Assert.NotNull(mock.Object);
        Assert.IsAssignableFrom<ITelemetryGauge>(mock.Object);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ITelemetryFactory_MockRoutesAllCreationMethods()
    {
        // OpenTelemetryContext, PostCopy, CreateCounter, CreateGauge.
        Mock<ITelemetryContext> contextMock = new();
        Mock<ITelemetryCounter> counterMock = new();
        Mock<ITelemetryGauge> gaugeMock = new();

        Mock<ITelemetryFactory> factoryMock = new();
        factoryMock
            .Setup(f => f.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel))
            .Returns(contextMock.Object);
        factoryMock
            .Setup(f => f.CreateCounter("writerFailover.triggered.count"))
            .Returns(counterMock.Object);
        factoryMock
            .Setup(f => f.CreateGauge("iam.tokenCache.size", It.IsAny<Func<long>>()))
            .Returns(gaugeMock.Object);

        ITelemetryFactory factory = factoryMock.Object;

        ITelemetryContext ctx = factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);
        ITelemetryCounter ctr = factory.CreateCounter("writerFailover.triggered.count");
        ITelemetryGauge gauge = factory.CreateGauge("iam.tokenCache.size", () => 42L);
        factory.PostCopy(ctx, TelemetryTraceLevel.ForceTopLevel);

        Assert.Same(contextMock.Object, ctx);
        Assert.Same(counterMock.Object, ctr);
        Assert.Same(gaugeMock.Object, gauge);

        factoryMock.Verify(
            f => f.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel), Times.Once);
        factoryMock.Verify(
            f => f.PostCopy(contextMock.Object, TelemetryTraceLevel.ForceTopLevel), Times.Once);
        factoryMock.Verify(
            f => f.CreateCounter("writerFailover.triggered.count"), Times.Once);
        factoryMock.Verify(
            f => f.CreateGauge("iam.tokenCache.size", It.IsAny<Func<long>>()), Times.Once);
    }
}
