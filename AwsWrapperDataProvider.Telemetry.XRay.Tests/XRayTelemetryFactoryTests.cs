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

using Amazon.XRay.Recorder.Core;
using Amazon.XRay.Recorder.Core.Internal.Entities;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;

namespace AwsWrapperDataProvider.Telemetry.XRay.Tests;

/// <summary>
/// Unit tests for <see cref="XRayTelemetryFactory"/> and
/// <see cref="XRayTelemetryLoader"/>. Verifies trace-context creation,
/// metric operations reject, and the PostCopy cloning behaviour.
/// </summary>
[Collection("X-Ray serial")]
public class XRayTelemetryFactoryTests : IDisposable
{
    private readonly XRayTestHarness harness;

    public XRayTelemetryFactoryTests()
    {
        this.harness = new XRayTestHarness();
    }

    public void Dispose() => this.harness.Dispose();

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenTelemetryContext_TopLevel_ReturnsXRayContext()
    {
        XRayTelemetryFactory factory = new();

        ITelemetryContext ctx = factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);
        try
        {
            Assert.IsType<XRayTelemetryContext>(ctx);
        }
        finally
        {
            ctx.CloseContext();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void OpenTelemetryContext_NoTrace_ReturnsNoOp()
    {
        XRayTelemetryFactory factory = new();

        ITelemetryContext ctx = factory.OpenTelemetryContext("op", TelemetryTraceLevel.NoTrace);
        try
        {
            // XRayTelemetryContext with NoTrace has a null Entity; verify via type still.
            XRayTelemetryContext xr = Assert.IsType<XRayTelemetryContext>(ctx);
            Assert.Null(xr.Entity);
        }
        finally
        {
            ctx.CloseContext();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateCounter_ThrowsNotSupported()
    {
        XRayTelemetryFactory factory = new();

        NotSupportedException ex = Assert.Throws<NotSupportedException>(
            () => factory.CreateCounter("any.counter"));
        Assert.Contains("X-Ray", ex.Message);
        Assert.Contains("metrics", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CreateGauge_ThrowsNotSupported()
    {
        XRayTelemetryFactory factory = new();

        NotSupportedException ex = Assert.Throws<NotSupportedException>(
            () => factory.CreateGauge("any.gauge", () => 0L));
        Assert.Contains("X-Ray", ex.Message);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PostCopy_MismatchedContextType_IsNoOp()
    {
        XRayTelemetryFactory factory = new();

        // NullTelemetryContext is the core Null Object; passing it must not
        // throw or emit anything.
        factory.PostCopy(NullTelemetryContext.Instance, TelemetryTraceLevel.ForceTopLevel);

        Assert.Empty(this.harness.Emitter.Sent);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PostCopy_NoTrace_IsNoOp()
    {
        XRayTelemetryFactory factory = new();

        ITelemetryContext ctx = factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);
        ctx.CloseContext();
        int beforeCount = this.harness.Emitter.Sent.Count;

        factory.PostCopy(ctx, TelemetryTraceLevel.NoTrace);

        Assert.Equal(beforeCount, this.harness.Emitter.Sent.Count);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PostCopy_SourceIsNoOp_DoesNothing()
    {
        XRayTelemetryFactory factory = new();

        // Source opened as NoTrace — its Entity is null.
        ITelemetryContext ctx = factory.OpenTelemetryContext("op", TelemetryTraceLevel.NoTrace);
        factory.PostCopy(ctx, TelemetryTraceLevel.ForceTopLevel);

        Assert.Empty(this.harness.Emitter.Sent);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PostCopy_ForceTopLevel_EmitsIndependentRootSegment_PreservingTiming()
    {
        XRayTelemetryFactory factory = new();

        // Open a top-level context, set an attribute, and close it.
        XRayTelemetryContext source = (XRayTelemetryContext)factory.OpenTelemetryContext(
            "op.orig", TelemetryTraceLevel.TopLevel);
        Assert.NotNull(source.Entity);
        source.SetAttribute("custom", "value42");

        decimal sourceStart = source.Entity!.StartTime;
        string sourceTraceId = source.Entity.TraceId!;
        source.CloseContext();
        decimal sourceEnd = source.Entity.EndTime;

        // One segment emitted — the original.
        Assert.Single(this.harness.Emitter.Sent);
        Assert.Equal("op.orig", this.harness.Emitter.Sent[0].Name);

        // Emit the PostCopy clone.
        factory.PostCopy(source, TelemetryTraceLevel.ForceTopLevel);

        // Two segments now emitted: the original and the clone.
        IReadOnlyList<Entity> emitted = this.harness.Emitter.Sent;
        Assert.Equal(2, emitted.Count);

        Entity clone = emitted[1];
        Assert.Equal("copy: op.orig", clone.Name);
        Assert.Equal(sourceStart, clone.StartTime);

        // End time matches the source's end time at millisecond precision.
        decimal endDiffSeconds = Math.Abs(clone.EndTime - sourceEnd);
        Assert.True(endDiffSeconds < 0.01m, $"Clone end time {clone.EndTime} too far from source {sourceEnd}");

        // The custom attribute was copied and a sourceTraceId annotation
        // pointing back to the source trace was added.
        Dictionary<string, object> cloneAnnotations = clone.Annotations.ToDictionary();
        Assert.Equal("value42", cloneAnnotations["custom"]);
        Assert.Equal(sourceTraceId, cloneAnnotations[XRayTelemetryContext.SourceTraceAnnotation]);

        // The clone's traceName is its own "copy: …" name, not the source's.
        Assert.Equal("copy: op.orig", cloneAnnotations[XRayTelemetryContext.TraceNameAnnotation]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PostCopy_ForceTopLevel_CallerTraceContextUnchanged()
    {
        XRayTelemetryFactory factory = new();

        // The caller has an open parent segment at PostCopy time. After
        // PostCopy returns, the caller's trace context must still point at
        // the same parent — the clone emits as an independent root without
        // mutating the caller's AsyncLocal state.
        AWSXRayRecorder.Instance.BeginSegment("caller.parent");
        Entity callerParent = AWSXRayRecorder.Instance.GetEntity();

        XRayTelemetryContext source = (XRayTelemetryContext)factory.OpenTelemetryContext(
            "src.nested", TelemetryTraceLevel.Nested);
        source.CloseContext();

        // Sanity: still on the caller's parent after source closes.
        Assert.Same(callerParent, AWSXRayRecorder.Instance.GetEntity());

        factory.PostCopy(source, TelemetryTraceLevel.ForceTopLevel);

        Assert.True(AWSXRayRecorder.Instance.IsEntityPresent());
        Assert.Same(callerParent, AWSXRayRecorder.Instance.GetEntity());

        AWSXRayRecorder.Instance.EndSegment();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void PostCopy_Nested_EmitsSubsegmentInlineUnderCurrentParent()
    {
        XRayTelemetryFactory factory = new();

        AWSXRayRecorder.Instance.BeginSegment("root");
        try
        {
            XRayTelemetryContext source = (XRayTelemetryContext)factory.OpenTelemetryContext(
                "nested.orig", TelemetryTraceLevel.Nested);
            Assert.NotNull(source.Entity);
            source.SetAttribute("nested-key", "nested-value");
            source.CloseContext();

            factory.PostCopy(source, TelemetryTraceLevel.Nested);
        }
        finally
        {
            AWSXRayRecorder.Instance.EndSegment();
        }

        // The root has emitted; the copy lives as a subsegment under it.
        Entity root = Assert.Single(this.harness.Emitter.Sent);
        List<string> subsegmentNames = root.Subsegments.Select(s => s.Name).ToList();
        Assert.Contains("nested.orig", subsegmentNames);
        Assert.Contains("copy: nested.orig", subsegmentNames);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Loader_Load_RegistersFactoryUnderXrayName()
    {
        XRayTelemetryLoader.Load();

        Dictionary<string, string> props = new()
        {
            ["EnableTelemetry"] = "true",
            ["TelemetryTracesBackend"] = XRayTelemetryLoader.BackendName,
            ["TelemetrySubmitTopLevel"] = "true",
        };
        DefaultTelemetryFactory factory = new(props);

        ITelemetryContext ctx = factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);
        try
        {
            Assert.IsType<XRayTelemetryContext>(ctx);
        }
        finally
        {
            ctx.CloseContext();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Loader_Load_IsIdempotent()
    {
        XRayTelemetryLoader.Load();
        XRayTelemetryLoader.Load();
        XRayTelemetryLoader.Load();

        Dictionary<string, string> props = new()
        {
            ["EnableTelemetry"] = "true",
            ["TelemetryTracesBackend"] = XRayTelemetryLoader.BackendName,
            ["TelemetrySubmitTopLevel"] = "true",
        };
        DefaultTelemetryFactory factory = new(props);

        ITelemetryContext ctx = factory.OpenTelemetryContext("op", TelemetryTraceLevel.TopLevel);
        try
        {
            Assert.IsType<XRayTelemetryContext>(ctx);
        }
        finally
        {
            ctx.CloseContext();
        }
    }
}
