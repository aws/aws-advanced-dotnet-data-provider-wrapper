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
using System.Diagnostics.Metrics;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;

namespace AwsWrapperDataProvider.Tests.Driver.Utils.Telemetry;

/// <summary>
/// Tests for <see cref="OtlpTelemetryFactory"/> and its backing types.
/// Uses <see cref="ActivityListener"/> and <see cref="MeterListener"/> to
/// capture activities and measurements in-process without requiring an
/// external OTLP collector.
/// </summary>
public class OtlpTelemetryFactoryTests
{
    private static ActivityListener CreateActivityListener(List<Activity> started, List<Activity> stopped)
    {
        ActivityListener listener = new()
        {
            ShouldListenTo = source => source.Name == OtlpTelemetryFactory.InstrumentationName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = a => started.Add(a),
            ActivityStopped = a => stopped.Add(a),
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Instance_IsSingleton()
    {
        Assert.NotNull(OtlpTelemetryFactory.Instance);
        Assert.Same(OtlpTelemetryFactory.Instance, OtlpTelemetryFactory.Instance);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void InstrumentationName_MatchesSpec()
    {
        Assert.Equal("Amazon.AwsWrapperDataProvider", OtlpTelemetryFactory.InstrumentationName);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenTelemetryContext_TopLevel_NoParent_CreatesRootActivity()
    {
        Assert.Null(Activity.Current);
        List<Activity> started = new();
        List<Activity> stopped = new();
        using ActivityListener listener = CreateActivityListener(started, stopped);

        ITelemetryContext ctx = OtlpTelemetryFactory.Instance
            .OpenTelemetryContext("op.top", TelemetryTraceLevel.TopLevel);

        Assert.IsType<OtlpTelemetryContext>(ctx);
        Assert.Single(started);
        Assert.Equal("op.top", started[0].DisplayName);
        Assert.Null(started[0].Parent);

        ctx.CloseContext();
        Assert.Single(stopped);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenTelemetryContext_TopLevel_WithParent_ReturnsNullContext()
    {
        List<Activity> started = new();
        List<Activity> stopped = new();
        using ActivityListener listener = CreateActivityListener(started, stopped);

        Activity parent = new Activity("external.parent").Start();
        try
        {
            ITelemetryContext ctx = OtlpTelemetryFactory.Instance
                .OpenTelemetryContext("op.top", TelemetryTraceLevel.TopLevel);

            Assert.Same(NullTelemetryContext.Instance, ctx);
            Assert.Empty(started);
        }
        finally
        {
            parent.Stop();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenTelemetryContext_ForceTopLevel_NoParent_CreatesRootActivity()
    {
        Assert.Null(Activity.Current);
        List<Activity> started = new();
        List<Activity> stopped = new();
        using ActivityListener listener = CreateActivityListener(started, stopped);

        ITelemetryContext ctx = OtlpTelemetryFactory.Instance
            .OpenTelemetryContext("op.force", TelemetryTraceLevel.ForceTopLevel);

        Assert.IsType<OtlpTelemetryContext>(ctx);
        Assert.Single(started);
        Assert.Null(started[0].Parent);

        ctx.CloseContext();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenTelemetryContext_Nested_NoParent_ReturnsNullContext()
    {
        Assert.Null(Activity.Current);
        List<Activity> started = new();
        List<Activity> stopped = new();
        using ActivityListener listener = CreateActivityListener(started, stopped);

        ITelemetryContext ctx = OtlpTelemetryFactory.Instance
            .OpenTelemetryContext("op.nested", TelemetryTraceLevel.Nested);

        Assert.Same(NullTelemetryContext.Instance, ctx);
        Assert.Empty(started);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenTelemetryContext_Nested_WithParent_CreatesChildActivity()
    {
        List<Activity> started = new();
        List<Activity> stopped = new();
        using ActivityListener listener = CreateActivityListener(started, stopped);

        Activity parent = new Activity("external.parent").Start();
        try
        {
            ITelemetryContext ctx = OtlpTelemetryFactory.Instance
                .OpenTelemetryContext("op.nested", TelemetryTraceLevel.Nested);

            Assert.IsType<OtlpTelemetryContext>(ctx);
            Assert.Single(started);
            Assert.Equal("op.nested", started[0].DisplayName);
            Assert.Same(parent, started[0].Parent);

            ctx.CloseContext();
        }
        finally
        {
            parent.Stop();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenTelemetryContext_NoTrace_AlwaysReturnsNullContext()
    {
        List<Activity> started = new();
        List<Activity> stopped = new();
        using ActivityListener listener = CreateActivityListener(started, stopped);

        ITelemetryContext noParent = OtlpTelemetryFactory.Instance
            .OpenTelemetryContext("op.no", TelemetryTraceLevel.NoTrace);
        Assert.Same(NullTelemetryContext.Instance, noParent);

        Activity parent = new Activity("external.parent").Start();
        try
        {
            ITelemetryContext withParent = OtlpTelemetryFactory.Instance
                .OpenTelemetryContext("op.no", TelemetryTraceLevel.NoTrace);
            Assert.Same(NullTelemetryContext.Instance, withParent);
        }
        finally
        {
            parent.Stop();
        }

        Assert.Empty(started);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Context_SetAttribute_SetsTagOnActivity()
    {
        List<Activity> started = new();
        List<Activity> stopped = new();
        using ActivityListener listener = CreateActivityListener(started, stopped);

        ITelemetryContext ctx = OtlpTelemetryFactory.Instance
            .OpenTelemetryContext("op.attr", TelemetryTraceLevel.TopLevel);
        ctx.SetAttribute("db.system", "mysql");
        ctx.CloseContext();

        Activity a = Assert.Single(stopped);
        KeyValuePair<string, object?> tag = Assert.Single(
            a.TagObjects,
            kv => kv.Key == "db.system");
        Assert.Equal("mysql", tag.Value);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Context_SetSuccess_SetsSuccessTag()
    {
        List<Activity> started = new();
        List<Activity> stopped = new();
        using ActivityListener listener = CreateActivityListener(started, stopped);

        ITelemetryContext ctx = OtlpTelemetryFactory.Instance
            .OpenTelemetryContext("op.success", TelemetryTraceLevel.TopLevel);
        ctx.SetSuccess(true);
        ctx.CloseContext();

        Activity a = Assert.Single(stopped);
        KeyValuePair<string, object?> tag = Assert.Single(
            a.TagObjects,
            kv => kv.Key == OtlpTelemetryContext.SuccessTagName);
        Assert.Equal(true, tag.Value);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Context_SetException_RecordsEventAndErrorStatus()
    {
        List<Activity> started = new();
        List<Activity> stopped = new();
        using ActivityListener listener = CreateActivityListener(started, stopped);

        ITelemetryContext ctx = OtlpTelemetryFactory.Instance
            .OpenTelemetryContext("op.error", TelemetryTraceLevel.TopLevel);
        InvalidOperationException ex = new("boom");
        ctx.SetException(ex);
        ctx.CloseContext();

        Activity a = Assert.Single(stopped);
        Assert.Equal(ActivityStatusCode.Error, a.Status);
        Assert.Equal("boom", a.StatusDescription);

        ActivityEvent evt = Assert.Single(a.Events);
        Assert.Equal(OtlpTelemetryContext.ExceptionEventName, evt.Name);
        Dictionary<string, object?> evtTags = evt.Tags.ToDictionary(t => t.Key, t => t.Value);
        Assert.Equal(typeof(InvalidOperationException).FullName, evtTags["exception.type"]);
        Assert.Equal("boom", evtTags["exception.message"]);
        Assert.Contains("InvalidOperationException", (string)evtTags["exception.stacktrace"]!);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Context_GetName_ReturnsNamePassedAtCreation()
    {
        List<Activity> started = new();
        List<Activity> stopped = new();
        using ActivityListener listener = CreateActivityListener(started, stopped);

        ITelemetryContext ctx = OtlpTelemetryFactory.Instance
            .OpenTelemetryContext("custom-name", TelemetryTraceLevel.TopLevel);
        Assert.Equal("custom-name", ctx.GetName());
        ctx.CloseContext();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Context_CloseContext_StopsActivityAndSetsDuration()
    {
        List<Activity> started = new();
        List<Activity> stopped = new();
        using ActivityListener listener = CreateActivityListener(started, stopped);

        ITelemetryContext ctx = OtlpTelemetryFactory.Instance
            .OpenTelemetryContext("op.stop", TelemetryTraceLevel.TopLevel);
        Thread.Sleep(5);
        ctx.CloseContext();

        Activity a = Assert.Single(stopped);
        Assert.True(a.Duration > TimeSpan.Zero);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PostCopy_CreatesIndependentCopyActivity_WithOriginalTimesAndTags()
    {
        List<Activity> started = new();
        List<Activity> stopped = new();
        using ActivityListener listener = CreateActivityListener(started, stopped);

        ITelemetryContext ctx = OtlpTelemetryFactory.Instance
            .OpenTelemetryContext("op.copy-src", TelemetryTraceLevel.TopLevel);
        ctx.SetAttribute("db.system", "mysql");
        ctx.SetSuccess(true);
        Thread.Sleep(5);
        ctx.CloseContext();

        OtlpTelemetryContext otlpCtx = Assert.IsType<OtlpTelemetryContext>(ctx);
        DateTime originalStart = otlpCtx.StartTime;
        DateTime originalEnd = otlpCtx.EndTime;

        OtlpTelemetryFactory.Instance.PostCopy(ctx, TelemetryTraceLevel.ForceTopLevel);

        Assert.Equal(2, stopped.Count);
        Activity copy = stopped.Single(a => a.DisplayName == "copy: op.copy-src");
        Assert.Null(copy.Parent);
        Assert.Equal(originalStart, copy.StartTimeUtc);
        Assert.Equal(originalEnd, copy.StartTimeUtc + copy.Duration);

        Dictionary<string, object?> tags = copy.TagObjects.ToDictionary(t => t.Key, t => t.Value);
        Assert.Equal("mysql", tags["db.system"]);
        Assert.Equal(true, tags[OtlpTelemetryContext.SuccessTagName]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PostCopy_WhenSourceIsNullContext_IsNoOp()
    {
        List<Activity> started = new();
        List<Activity> stopped = new();
        using ActivityListener listener = CreateActivityListener(started, stopped);

        Exception? caught = Record.Exception(() =>
            OtlpTelemetryFactory.Instance.PostCopy(
                NullTelemetryContext.Instance, TelemetryTraceLevel.ForceTopLevel));

        Assert.Null(caught);
        Assert.Empty(started);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateCounter_ReturnsOtlpCounter_AndEmitsMeasurements()
    {
        string counterName = "otlp.test.counter." + Guid.NewGuid().ToString("N");
        List<long> measurements = new();
        using MeterListener meterListener = new();
        meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == OtlpTelemetryFactory.InstrumentationName
                && instrument.Name == counterName)
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        meterListener.SetMeasurementEventCallback<long>(
            (_, value, _, _) => measurements.Add(value));
        meterListener.Start();

        ITelemetryCounter counter = OtlpTelemetryFactory.Instance.CreateCounter(counterName);
        Assert.IsType<OtlpTelemetryCounter>(counter);

        counter.Add(5);
        counter.Inc();
        counter.Add(10);

        Assert.Equal(new List<long> { 5, 1, 10 }, measurements);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateGauge_ReturnsOtlpGauge_AndCallbackIsInvokedOnObservation()
    {
        string gaugeName = "otlp.test.gauge." + Guid.NewGuid().ToString("N");
        int callbackInvocations = 0;
        long currentValue = 42;

        List<long> measurements = new();
        using MeterListener meterListener = new();
        meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == OtlpTelemetryFactory.InstrumentationName
                && instrument.Name == gaugeName)
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        meterListener.SetMeasurementEventCallback<long>(
            (_, value, _, _) => measurements.Add(value));
        meterListener.Start();

        ITelemetryGauge gauge = OtlpTelemetryFactory.Instance.CreateGauge(
            gaugeName,
            () =>
            {
                callbackInvocations++;
                return currentValue;
            });
        Assert.IsType<OtlpTelemetryGauge>(gauge);

        meterListener.RecordObservableInstruments();
        Assert.Equal(1, callbackInvocations);
        Assert.Single(measurements);
        Assert.Equal(42, measurements[0]);

        currentValue = 99;
        meterListener.RecordObservableInstruments();
        Assert.Equal(2, callbackInvocations);
        Assert.Equal(new List<long> { 42, 99 }, measurements);
    }
}
