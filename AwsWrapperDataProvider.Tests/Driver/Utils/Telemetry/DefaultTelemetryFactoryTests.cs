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

using System.Diagnostics;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Utils.Telemetry;

/// <summary>
/// Tests for <see cref="DefaultTelemetryFactory"/>. Verifies configuration
/// driven routing, the submitTopLevel trace-level adjustment, independent
/// trace/metric backend selection, and the static registration pattern used
/// by the X-Ray project.
/// </summary>
public class DefaultTelemetryFactoryTests
{
    private const string EnableKey = "EnableTelemetry";
    private const string TracesKey = "TelemetryTracesBackend";
    private const string MetricsKey = "TelemetryMetricsBackend";
    private const string SubmitTopLevelKey = "TelemetrySubmitTopLevel";

    private static ActivityListener CreateActivityListener()
    {
        ActivityListener listener = new()
        {
            ShouldListenTo = source => source.Name == OtlpTelemetryFactory.InstrumentationName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }


    [Fact]
    [Trait("Category", "Unit")]
    public void Disabled_AllOperations_RouteToNullFactory()
    {
        Dictionary<string, string> props = new()
        {
            [EnableKey] = "false",
            [TracesKey] = "OTLP",
            [MetricsKey] = "OTLP",
        };

        DefaultTelemetryFactory factory = new(props);

        ITelemetryContext ctx = factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);
        ITelemetryCounter counter = factory.CreateCounter("any");
        ITelemetryGauge gauge = factory.CreateGauge("any", () => 0);

        Assert.Same(NullTelemetryContext.Instance, ctx);
        Assert.Same(NullTelemetryCounter.Instance, counter);
        Assert.Same(NullTelemetryGauge.Instance, gauge);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Disabled_WithNoTelemetryPropertiesAtAll_IsDisabledByDefault()
    {
        // Property defaults: EnableTelemetry=false, backends=NONE.
        DefaultTelemetryFactory factory = new(new Dictionary<string, string>());

        Assert.Same(
            NullTelemetryContext.Instance,
            factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel));
        Assert.Same(NullTelemetryCounter.Instance, factory.CreateCounter("any"));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Enabled_TracesBackendOtlp_CreatesActivityViaOtlpFactory()
    {
        Assert.Null(Activity.Current);
        using ActivityListener listener = CreateActivityListener();

        Dictionary<string, string> props = new()
        {
            [EnableKey] = "true",
            [TracesKey] = "OTLP",
            [SubmitTopLevelKey] = "true",
        };
        DefaultTelemetryFactory factory = new(props);

        ITelemetryContext ctx = factory.OpenTelemetryContext("op.otlp", TelemetryTraceLevel.TopLevel);

        Assert.IsType<OtlpTelemetryContext>(ctx);
        ctx.CloseContext();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Enabled_MetricsBackendOtlp_CreatesOtlpInstruments()
    {
        Dictionary<string, string> props = new()
        {
            [EnableKey] = "true",
            [MetricsKey] = "OTLP",
        };
        DefaultTelemetryFactory factory = new(props);

        ITelemetryCounter counter = factory.CreateCounter("default.test.counter." + Guid.NewGuid().ToString("N"));
        ITelemetryGauge gauge = factory.CreateGauge("default.test.gauge." + Guid.NewGuid().ToString("N"), () => 0);

        Assert.IsType<OtlpTelemetryCounter>(counter);
        Assert.IsType<OtlpTelemetryGauge>(gauge);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Enabled_TracesOtlp_MetricsNone_RoutesIndependently()
    {
        using ActivityListener listener = CreateActivityListener();

        Dictionary<string, string> props = new()
        {
            [EnableKey] = "true",
            [TracesKey] = "OTLP",
            [MetricsKey] = "NONE",
            [SubmitTopLevelKey] = "true",
        };
        DefaultTelemetryFactory factory = new(props);

        ITelemetryContext ctx = factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);
        ITelemetryCounter counter = factory.CreateCounter("irrelevant");

        Assert.IsType<OtlpTelemetryContext>(ctx);
        Assert.Same(NullTelemetryCounter.Instance, counter);
        ctx.CloseContext();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Enabled_TracesNone_MetricsOtlp_RoutesIndependently()
    {
        Dictionary<string, string> props = new()
        {
            [EnableKey] = "true",
            [TracesKey] = "NONE",
            [MetricsKey] = "OTLP",
            [SubmitTopLevelKey] = "true",
        };
        DefaultTelemetryFactory factory = new(props);

        ITelemetryContext ctx = factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);
        ITelemetryCounter counter = factory.CreateCounter("default.test.counter." + Guid.NewGuid().ToString("N"));

        Assert.Same(NullTelemetryContext.Instance, ctx);
        Assert.IsType<OtlpTelemetryCounter>(counter);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("BOGUS")]
    [InlineData("xray")] // XRAY without registration is treated as NONE
    [InlineData("")]
    public void Enabled_UnrecognizedTracesBackend_FallsBackToNull(string backend)
    {
        Dictionary<string, string> props = new()
        {
            [EnableKey] = "true",
            [TracesKey] = backend,
            [SubmitTopLevelKey] = "true",
        };
        DefaultTelemetryFactory factory = new(props);

        ITelemetryContext ctx = factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);

        Assert.Same(NullTelemetryContext.Instance, ctx);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Enabled_UnrecognizedMetricsBackend_FallsBackToNull()
    {
        Dictionary<string, string> props = new()
        {
            [EnableKey] = "true",
            [MetricsKey] = "BOGUS",
        };
        DefaultTelemetryFactory factory = new(props);

        Assert.Same(NullTelemetryCounter.Instance, factory.CreateCounter("any"));
        Assert.Same(NullTelemetryGauge.Instance, factory.CreateGauge("any", () => 0));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RegisterTelemetryFactory_RoutesToRegisteredBackend()
    {
        string backendName = "TEST" + Guid.NewGuid().ToString("N");
        Mock<ITelemetryFactory> mockFactory = new();
        Mock<ITelemetryContext> mockContext = new();
        mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(mockContext.Object);

        DefaultTelemetryFactory.RegisterTelemetryFactory(backendName, mockFactory.Object);
        try
        {
            Dictionary<string, string> props = new()
            {
                [EnableKey] = "true",
                [TracesKey] = backendName,
                [SubmitTopLevelKey] = "true",
            };
            DefaultTelemetryFactory factory = new(props);

            ITelemetryContext ctx = factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);

            Assert.Same(mockContext.Object, ctx);
            mockFactory.Verify(
                f => f.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel),
                Times.Once);
        }
        finally
        {
            DefaultTelemetryFactory.UnregisterTelemetryFactory(backendName);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RegisterTelemetryFactory_IsCaseInsensitive()
    {
        string backendName = "FooBar" + Guid.NewGuid().ToString("N");
        Mock<ITelemetryFactory> mockFactory = new();
        mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(NullTelemetryContext.Instance);

        DefaultTelemetryFactory.RegisterTelemetryFactory(backendName.ToLowerInvariant(), mockFactory.Object);
        try
        {
            Dictionary<string, string> props = new()
            {
                [EnableKey] = "true",
                [TracesKey] = backendName.ToUpperInvariant(),
                [SubmitTopLevelKey] = "true",
            };
            DefaultTelemetryFactory factory = new(props);
            factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);

            mockFactory.Verify(
                f => f.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel),
                Times.Once);
        }
        finally
        {
            DefaultTelemetryFactory.UnregisterTelemetryFactory(backendName);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PostCopy_DelegatesToTracesFactory_NotMetricsFactory()
    {
        string tracesName = "TRACES" + Guid.NewGuid().ToString("N");
        string metricsName = "METRICS" + Guid.NewGuid().ToString("N");
        Mock<ITelemetryFactory> tracesMock = new();
        Mock<ITelemetryFactory> metricsMock = new();

        DefaultTelemetryFactory.RegisterTelemetryFactory(tracesName, tracesMock.Object);
        DefaultTelemetryFactory.RegisterTelemetryFactory(metricsName, metricsMock.Object);
        try
        {
            Dictionary<string, string> props = new()
            {
                [EnableKey] = "true",
                [TracesKey] = tracesName,
                [MetricsKey] = metricsName,
            };
            DefaultTelemetryFactory factory = new(props);

            factory.PostCopy(NullTelemetryContext.Instance, TelemetryTraceLevel.ForceTopLevel);

            tracesMock.Verify(
                f => f.PostCopy(NullTelemetryContext.Instance, TelemetryTraceLevel.ForceTopLevel),
                Times.Once);
            metricsMock.Verify(
                f => f.PostCopy(It.IsAny<ITelemetryContext>(), It.IsAny<TelemetryTraceLevel>()),
                Times.Never);
        }
        finally
        {
            DefaultTelemetryFactory.UnregisterTelemetryFactory(tracesName);
            DefaultTelemetryFactory.UnregisterTelemetryFactory(metricsName);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void UnregisterTelemetryFactory_PreventsFurtherRouting()
    {
        string backendName = "ONCE" + Guid.NewGuid().ToString("N");
        Mock<ITelemetryFactory> mockFactory = new();
        DefaultTelemetryFactory.RegisterTelemetryFactory(backendName, mockFactory.Object);
        DefaultTelemetryFactory.UnregisterTelemetryFactory(backendName);

        Dictionary<string, string> props = new()
        {
            [EnableKey] = "true",
            [TracesKey] = backendName,
            [SubmitTopLevelKey] = "true",
        };
        DefaultTelemetryFactory factory = new(props);

        ITelemetryContext ctx = factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);

        Assert.Same(NullTelemetryContext.Instance, ctx);
        mockFactory.Verify(
            f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()),
            Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SubmitTopLevelFalse_ConvertsTopLevelToNested()
    {
        string backendName = "DOWNGRADE" + Guid.NewGuid().ToString("N");
        Mock<ITelemetryFactory> mockFactory = new();
        mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(NullTelemetryContext.Instance);

        DefaultTelemetryFactory.RegisterTelemetryFactory(backendName, mockFactory.Object);
        try
        {
            Dictionary<string, string> props = new()
            {
                [EnableKey] = "true",
                [TracesKey] = backendName,
                [SubmitTopLevelKey] = "false",
            };
            DefaultTelemetryFactory factory = new(props);
            factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);

            // Downgrade: TopLevel passed in → Nested delegated.
            mockFactory.Verify(
                f => f.OpenTelemetryContext("op", TelemetryTraceLevel.Nested),
                Times.Once);
            mockFactory.Verify(
                f => f.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel),
                Times.Never);
        }
        finally
        {
            DefaultTelemetryFactory.UnregisterTelemetryFactory(backendName);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SubmitTopLevelTrue_KeepsTopLevelAsTopLevel()
    {
        string backendName = "KEEP" + Guid.NewGuid().ToString("N");
        Mock<ITelemetryFactory> mockFactory = new();
        mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(NullTelemetryContext.Instance);

        DefaultTelemetryFactory.RegisterTelemetryFactory(backendName, mockFactory.Object);
        try
        {
            Dictionary<string, string> props = new()
            {
                [EnableKey] = "true",
                [TracesKey] = backendName,
                [SubmitTopLevelKey] = "true",
            };
            DefaultTelemetryFactory factory = new(props);
            factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);

            mockFactory.Verify(
                f => f.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel),
                Times.Once);
        }
        finally
        {
            DefaultTelemetryFactory.UnregisterTelemetryFactory(backendName);
        }
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(TelemetryTraceLevel.ForceTopLevel)]
    [InlineData(TelemetryTraceLevel.Nested)]
    [InlineData(TelemetryTraceLevel.NoTrace)]
    public void SubmitTopLevelFalse_DoesNotAffectOtherLevels(TelemetryTraceLevel level)
    {
        string backendName = "PASSTHROUGH" + Guid.NewGuid().ToString("N");
        Mock<ITelemetryFactory> mockFactory = new();
        mockFactory
            .Setup(f => f.OpenTelemetryContext(It.IsAny<string>(), It.IsAny<TelemetryTraceLevel>()))
            .Returns(NullTelemetryContext.Instance);

        DefaultTelemetryFactory.RegisterTelemetryFactory(backendName, mockFactory.Object);
        try
        {
            Dictionary<string, string> props = new()
            {
                [EnableKey] = "true",
                [TracesKey] = backendName,
                [SubmitTopLevelKey] = "false",
            };
            DefaultTelemetryFactory factory = new(props);
            factory.OpenTelemetryContext("op", level);

            mockFactory.Verify(
                f => f.OpenTelemetryContext("op", level),
                Times.Once);
        }
        finally
        {
            DefaultTelemetryFactory.UnregisterTelemetryFactory(backendName);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Factory_ImplementsITelemetryFactory()
    {
        DefaultTelemetryFactory factory = new(new Dictionary<string, string>());
        Assert.IsAssignableFrom<ITelemetryFactory>(factory);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PropertyDefinition_TelemetryProperties_HaveExpectedDefaults()
    {
        Assert.Equal("false", PropertyDefinition.EnableTelemetry.DefaultValue);
        Assert.Equal("NONE", PropertyDefinition.TelemetryTracesBackend.DefaultValue);
        Assert.Equal("NONE", PropertyDefinition.TelemetryMetricsBackend.DefaultValue);
        Assert.Equal("false", PropertyDefinition.TelemetrySubmitTopLevel.DefaultValue);
        Assert.Equal("false", PropertyDefinition.TelemetryFailoverAdditionalTopTrace.DefaultValue);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PropertyDefinition_BackendProperties_DeclareChoices()
    {
        Assert.Equal(
            new[] { "OTLP", "XRAY", "NONE" },
            PropertyDefinition.TelemetryTracesBackend.Choices);
        Assert.Equal(
            new[] { "OTLP", "NONE" },
            PropertyDefinition.TelemetryMetricsBackend.Choices);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PropertyDefinition_TelemetryProperties_AreInternal()
    {
        // Telemetry properties must not be forwarded to the target driver.
        Assert.Contains(PropertyDefinition.EnableTelemetry, PropertyDefinition.InternalWrapperProperties);
        Assert.Contains(PropertyDefinition.TelemetryTracesBackend, PropertyDefinition.InternalWrapperProperties);
        Assert.Contains(PropertyDefinition.TelemetryMetricsBackend, PropertyDefinition.InternalWrapperProperties);
        Assert.Contains(PropertyDefinition.TelemetrySubmitTopLevel, PropertyDefinition.InternalWrapperProperties);
        Assert.Contains(
            PropertyDefinition.TelemetryFailoverAdditionalTopTrace,
            PropertyDefinition.InternalWrapperProperties);
    }
}
