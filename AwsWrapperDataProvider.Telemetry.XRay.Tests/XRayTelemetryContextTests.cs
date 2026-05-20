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
/// Unit tests for <see cref="XRayTelemetryContext"/>. Drives the context
/// directly (bypassing <see cref="XRayTelemetryFactory"/>) and asserts on
/// the X-Ray entity state exposed via the internal accessor.
/// </summary>
[Collection("X-Ray serial")]
public class XRayTelemetryContextTests : IDisposable
{
    private readonly XRayTestHarness harness;

    public XRayTelemetryContextTests()
    {
        this.harness = new XRayTestHarness();
    }

    public void Dispose() => this.harness.Dispose();

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_TopLevel_NoParent_CreatesSegment_WithTraceNameAnnotation()
    {
        XRayTelemetryContext ctx = new("op.top", TelemetryTraceLevel.TopLevel);
        try
        {
            Assert.NotNull(ctx.Entity);
            Assert.IsType<Segment>(ctx.Entity);
            Assert.True(ctx.IsSegment);
            Assert.Equal("op.top", ctx.GetName());
            Assert.True(ctx.Entity!.Annotations.ToDictionary().ContainsKey(XRayTelemetryContext.TraceNameAnnotation));
        }
        finally
        {
            ctx.CloseContext();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_ForceTopLevel_NoParent_CreatesSegment()
    {
        XRayTelemetryContext ctx = new("op.force", TelemetryTraceLevel.ForceTopLevel);
        try
        {
            Assert.NotNull(ctx.Entity);
            Assert.True(ctx.IsSegment);
        }
        finally
        {
            ctx.CloseContext();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_TopLevel_WithParent_StartsNewSegment()
    {
        // Backends are now straight appliers: trace-level resolution lives in
        // DefaultTelemetryFactory. The X-Ray SDK's BeginSegment always starts
        // a new top-level segment regardless of any pre-existing entity, so a
        // TopLevel request reaching the context here just succeeds.
        AWSXRayRecorder.Instance.BeginSegment("existing.parent");
        try
        {
            XRayTelemetryContext ctx = new("op.top", TelemetryTraceLevel.TopLevel);
            try
            {
                Assert.NotNull(ctx.Entity);
                Assert.IsType<Segment>(ctx.Entity);
                Assert.True(ctx.IsSegment);
            }
            finally
            {
                ctx.CloseContext();
            }
        }
        finally
        {
            AWSXRayRecorder.Instance.EndSegment();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_Nested_WithParent_CreatesSubsegment()
    {
        AWSXRayRecorder.Instance.BeginSegment("parent");
        try
        {
            XRayTelemetryContext ctx = new("op.nested", TelemetryTraceLevel.Nested);
            try
            {
                Assert.NotNull(ctx.Entity);
                Assert.IsType<Subsegment>(ctx.Entity);
                Assert.False(ctx.IsSegment);
            }
            finally
            {
                ctx.CloseContext();
            }
        }
        finally
        {
            AWSXRayRecorder.Instance.EndSegment();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_Nested_WithoutParent_DoesNotThrow()
    {
        // Backends are now straight appliers; routing logic for the
        // "Nested without parent" case lives in DefaultTelemetryFactory.
        // When the X-Ray SDK's BeginSubsegment is invoked with no parent
        // entity, the configured ContextMissingStrategy (LOG_ERROR in the
        // test harness) suppresses the error. The context must not throw
        // either way; behaviour beyond that is the SDK's responsibility.
        Exception? ex = Record.Exception(() =>
        {
            XRayTelemetryContext ctx = new("op.nested", TelemetryTraceLevel.Nested);
            ctx.CloseContext();
        });

        Assert.Null(ex);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_NoTrace_ReturnsNoOp()
    {
        XRayTelemetryContext ctx = new("op.none", TelemetryTraceLevel.NoTrace);
        try
        {
            Assert.Null(ctx.Entity);
        }
        finally
        {
            ctx.CloseContext();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SetSuccess_False_SetsHasErrorTrue()
    {
        XRayTelemetryContext ctx = new("op", TelemetryTraceLevel.TopLevel);
        try
        {
            ctx.SetSuccess(false);
            Assert.True(ctx.Entity!.HasError);
        }
        finally
        {
            ctx.CloseContext();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SetSuccess_True_SetsHasErrorFalse_DoesNotClearHasFault()
    {
        XRayTelemetryContext ctx = new("op", TelemetryTraceLevel.TopLevel);
        try
        {
            // Simulate a prior SetException that set HasFault.
            ctx.Entity!.HasFault = true;
            ctx.SetSuccess(true);

            Assert.False(ctx.Entity.HasError);
            Assert.True(ctx.Entity.HasFault); // Intentionally preserved.
        }
        finally
        {
            ctx.CloseContext();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SetAttribute_AddsAnnotation()
    {
        XRayTelemetryContext ctx = new("op", TelemetryTraceLevel.TopLevel);
        try
        {
            ctx.SetAttribute("key1", "value1");
            ctx.SetAttribute("key2", "value2");

            Dictionary<string, object> annotations = ctx.Entity!.Annotations.ToDictionary();
            Assert.Equal("value1", annotations["key1"]);
            Assert.Equal("value2", annotations["key2"]);
        }
        finally
        {
            ctx.CloseContext();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SetException_SetsAnnotations_AndHasFault()
    {
        XRayTelemetryContext ctx = new("op", TelemetryTraceLevel.TopLevel);
        try
        {
            InvalidOperationException ex = new("boom");
            ctx.SetException(ex);

            Dictionary<string, object> annotations = ctx.Entity!.Annotations.ToDictionary();
            Assert.Equal(nameof(InvalidOperationException), annotations[XRayTelemetryContext.ExceptionTypeAnnotation]);
            Assert.Equal("boom", annotations[XRayTelemetryContext.ExceptionMessageAnnotation]);
            Assert.True(ctx.Entity.HasFault);
        }
        finally
        {
            ctx.CloseContext();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SetException_NullException_IsNoOp()
    {
        XRayTelemetryContext ctx = new("op", TelemetryTraceLevel.TopLevel);
        try
        {
            ctx.SetException(null!);

            Dictionary<string, object> annotations = ctx.Entity!.Annotations.ToDictionary();
            Assert.False(annotations.ContainsKey(XRayTelemetryContext.ExceptionTypeAnnotation));
            Assert.False(ctx.Entity.HasFault);
        }
        finally
        {
            ctx.CloseContext();
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CloseContext_TopLevel_EmitsSegmentAndClearsTraceContext()
    {
        XRayTelemetryContext ctx = new("op.close", TelemetryTraceLevel.TopLevel);
        ctx.CloseContext();

        Assert.False(AWSXRayRecorder.Instance.IsEntityPresent());
        Assert.Single(this.harness.Emitter.Sent);
        Assert.Equal("op.close", this.harness.Emitter.Sent[0].Name);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CloseContext_Nested_EmitsOnlyRootSegmentOnRootClose()
    {
        AWSXRayRecorder.Instance.BeginSegment("root");
        XRayTelemetryContext nested = new("op.nested", TelemetryTraceLevel.Nested);
        nested.CloseContext();

        // The subsegment is not emitted until the root segment ends.
        Assert.Empty(this.harness.Emitter.Sent);

        AWSXRayRecorder.Instance.EndSegment();

        Entity sentRoot = Assert.Single(this.harness.Emitter.Sent);
        Assert.Equal("root", sentRoot.Name);
        Assert.Contains(sentRoot.Subsegments, s => s.Name == "op.nested");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void NoOpContext_AllMethodsAreSafe()
    {
        XRayTelemetryContext ctx = new("op.noop", TelemetryTraceLevel.NoTrace);

        // None of these should throw on a null-entity context.
        ctx.SetSuccess(true);
        ctx.SetSuccess(false);
        ctx.SetAttribute("key", "value");
        ctx.SetException(new InvalidOperationException("ignored"));
        Assert.Equal("op.noop", ctx.GetName());
        ctx.CloseContext();
        ctx.CloseContext(); // Idempotent close is safe.

        Assert.Empty(this.harness.Emitter.Sent);
    }
}

/// <summary>
/// Extension helper — the SDK's <c>Annotations</c> class is an
/// <c>IEnumerable&lt;KeyValuePair&lt;string, object&gt;&gt;</c> but does not
/// support O(1) lookup; materialize to a dictionary for assertions.
/// </summary>
internal static class XRayAnnotationsExtensions
{
    public static Dictionary<string, object> ToDictionary(
        this Amazon.XRay.Recorder.Core.Internal.Entities.Annotations annotations)
    {
        Dictionary<string, object> map = new();
        foreach (KeyValuePair<string, object> kvp in annotations)
        {
            map[kvp.Key] = kvp.Value;
        }

        return map;
    }
}
