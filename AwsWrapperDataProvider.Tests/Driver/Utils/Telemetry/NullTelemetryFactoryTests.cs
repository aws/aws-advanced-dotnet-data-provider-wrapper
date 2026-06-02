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

namespace AwsWrapperDataProvider.Tests.Driver.Utils.Telemetry;

/// <summary>
/// Tests for the Null Object telemetry implementations. Verifies that
/// singletons have stable identity, every method is a safe no-op, and the
/// factory returns the corresponding Null instrument singletons for every
/// trace level.
/// </summary>
public class NullTelemetryFactoryTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void NullTelemetryFactory_Instance_IsSingleton()
    {
        Assert.NotNull(NullTelemetryFactory.Instance);
        Assert.Same(NullTelemetryFactory.Instance, NullTelemetryFactory.Instance);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void NullTelemetryContext_Instance_IsSingleton()
    {
        Assert.NotNull(NullTelemetryContext.Instance);
        Assert.Same(NullTelemetryContext.Instance, NullTelemetryContext.Instance);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void NullTelemetryCounter_Instance_IsSingleton()
    {
        Assert.NotNull(NullTelemetryCounter.Instance);
        Assert.Same(NullTelemetryCounter.Instance, NullTelemetryCounter.Instance);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void NullTelemetryGauge_Instance_IsSingleton()
    {
        Assert.NotNull(NullTelemetryGauge.Instance);
        Assert.Same(NullTelemetryGauge.Instance, NullTelemetryGauge.Instance);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(TelemetryTraceLevel.ForceTopLevel)]
    [InlineData(TelemetryTraceLevel.TopLevel)]
    [InlineData(TelemetryTraceLevel.Nested)]
    [InlineData(TelemetryTraceLevel.NoTrace)]
    public void OpenTelemetryContext_ReturnsNullContextSingleton_ForEveryTraceLevel(
        TelemetryTraceLevel traceLevel)
    {
        ITelemetryContext context = NullTelemetryFactory.Instance.OpenTelemetryContext("op", traceLevel);

        Assert.Same(NullTelemetryContext.Instance, context);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateCounter_ReturnsNullCounterSingleton()
    {
        ITelemetryCounter counter = NullTelemetryFactory.Instance.CreateCounter("any.counter");

        Assert.Same(NullTelemetryCounter.Instance, counter);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateGauge_ReturnsNullGaugeSingleton_AndNeverInvokesCallback()
    {
        bool callbackInvoked = false;
        ITelemetryGauge gauge = NullTelemetryFactory.Instance.CreateGauge(
            "any.gauge",
            () =>
            {
                callbackInvoked = true;
                return 99L;
            });

        Assert.Same(NullTelemetryGauge.Instance, gauge);
        Assert.False(callbackInvoked);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void NullTelemetryContext_AllMethods_AreNoOp_AndDoNotThrow()
    {
        ITelemetryContext context = NullTelemetryContext.Instance;

        Exception? caught = Record.Exception(() =>
        {
            context.SetSuccess(true);
            context.SetSuccess(false);
            context.SetAttribute("db.system", "mysql");
            context.SetException(new InvalidOperationException("boom"));
            context.CloseContext();

            // Calling CloseContext twice must still be safe.
            context.CloseContext();
        });

        Assert.Null(caught);
        Assert.Equal("NullTelemetryContext", context.GetName());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void NullTelemetryCounter_AddAndInc_AreNoOp_AndDoNotThrow()
    {
        ITelemetryCounter counter = NullTelemetryCounter.Instance;

        Exception? caught = Record.Exception(() =>
        {
            counter.Add(0);
            counter.Add(long.MaxValue);
            counter.Inc();
            counter.Inc();
        });

        Assert.Null(caught);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PostCopy_IsNoOp_AndDoesNotThrow()
    {
        Exception? caught = Record.Exception(() =>
            NullTelemetryFactory.Instance.PostCopy(
                NullTelemetryContext.Instance, TelemetryTraceLevel.ForceTopLevel));

        Assert.Null(caught);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void NullTelemetryFactory_ImplementsITelemetryFactory()
    {
        Assert.IsAssignableFrom<ITelemetryFactory>(NullTelemetryFactory.Instance);
        Assert.IsAssignableFrom<ITelemetryContext>(NullTelemetryContext.Instance);
        Assert.IsAssignableFrom<ITelemetryCounter>(NullTelemetryCounter.Instance);
        Assert.IsAssignableFrom<ITelemetryGauge>(NullTelemetryGauge.Instance);
    }
}
