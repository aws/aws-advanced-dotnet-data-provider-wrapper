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

namespace AwsWrapperDataProvider.Telemetry.XRay;

/// <summary>
/// X-Ray-backed <see cref="ITelemetryContext"/> implementation that wraps an
/// X-Ray <see cref="Entity"/> (either a <see cref="Segment"/> or a
/// <see cref="Subsegment"/>). Delegates attribute and exception recording to
/// the X-Ray SDK's native APIs on <see cref="AWSXRayRecorder"/>.
/// </summary>
public sealed class XRayTelemetryContext : ITelemetryContext
{
    /// <summary>Annotation key recording the span name.</summary>
    internal const string TraceNameAnnotation = "traceName";

    /// <summary>Annotation key recording the trace id of a parent segment.</summary>
    internal const string ParentTraceAnnotation = "parentTraceId";

    /// <summary>Annotation key recording the trace id of the source segment
    /// on a <c>PostCopy</c> clone.</summary>
    internal const string SourceTraceAnnotation = "sourceTraceId";

    /// <summary>Annotation key recording the simple name of an exception set
    /// via <see cref="SetException"/>.</summary>
    internal const string ExceptionTypeAnnotation = "exceptionType";

    /// <summary>Annotation key recording the message of an exception set via
    /// <see cref="SetException"/>.</summary>
    internal const string ExceptionMessageAnnotation = "exceptionMessage";

    private readonly string name;
    private readonly bool isSegment;
    private readonly Entity? entity;

    /// <summary>
    /// Initializes a new instance of the <see cref="XRayTelemetryContext"/>
    /// class and opens a new X-Ray segment, subsegment, or no-op context.
    /// </summary>
    /// <remarks>
    /// Trace-level resolution is performed by
    /// <see cref="Driver.Utils.Telemetry.DefaultTelemetryFactory"/>; this
    /// constructor trusts the caller's level and does not re-check parent
    /// presence. <c>BeginSegment</c> always opens a top-level segment;
    /// <c>BeginSubsegment</c> attaches to the current entity if one exists
    /// or is treated by the X-Ray SDK as a context-missing condition
    /// otherwise.
    /// </remarks>
    /// <param name="name">The span name.</param>
    /// <param name="traceLevel">The requested trace level.</param>
    public XRayTelemetryContext(string name, TelemetryTraceLevel traceLevel)
    {
        this.name = name;
        AWSXRayRecorder recorder = AWSXRayRecorder.Instance;

        switch (traceLevel)
        {
            case TelemetryTraceLevel.ForceTopLevel:
            case TelemetryTraceLevel.TopLevel:
                recorder.BeginSegment(name);
                this.entity = SafeGetEntity(recorder);
                this.isSegment = true;
                this.SetAttribute(TraceNameAnnotation, name);
                break;

            case TelemetryTraceLevel.Nested:
                recorder.BeginSubsegment(name);
                this.entity = SafeGetEntity(recorder);
                this.isSegment = false;
                this.SetAttribute(TraceNameAnnotation, name);
                break;

            case TelemetryTraceLevel.NoTrace:
            default:
                this.entity = null;
                break;
        }
    }

    /// <summary>
    /// Gets the underlying X-Ray entity, or <see langword="null"/> when this
    /// context is a no-op.
    /// </summary>
    internal Entity? Entity => this.entity;

    /// <summary>
    /// Gets a value indicating whether this context wraps a top-level
    /// segment (<see langword="true"/>) versus a nested subsegment
    /// (<see langword="false"/>). Only meaningful when
    /// <see cref="Entity"/> is non-null.
    /// </summary>
    internal bool IsSegment => this.isSegment;

    /// <inheritdoc />
    public void SetSuccess(bool success)
    {
        if (this.entity == null)
        {
            return;
        }

        // We leave HasFault alone so that a prior SetException that set HasFault is not
        // silently cleared by a subsequent SetSuccess(true) call.
        this.entity.HasError = !success;
    }

    /// <inheritdoc />
    public void SetAttribute(string key, string value)
    {
        if (this.entity == null)
        {
            return;
        }

        try
        {
            this.entity.AddAnnotation(key, value);
        }
        catch
        {
            // Telemetry failures must not break database operations. Invalid
            // key/value types or annotation-count overflow are swallowed.
        }
    }

    /// <inheritdoc />
    public void SetException(Exception exception)
    {
        if (this.entity == null || exception == null)
        {
            return;
        }

        // Record searchable annotations
        this.SetAttribute(ExceptionTypeAnnotation, exception.GetType().Name);
        string? message = exception.Message;
        if (!string.IsNullOrEmpty(message))
        {
            this.SetAttribute(ExceptionMessageAnnotation, message);
        }

        // Also record via the X-Ray native cause mechanism so the exception
        // surfaces in the X-Ray console's exception viewer. AddException on
        // the entity also sets HasFault = true.
        try
        {
            this.entity.AddException(exception);
        }
        catch
        {
            // Telemetry failures must not break database operations.
        }
    }

    /// <inheritdoc />
    public string GetName() => this.name;

    /// <inheritdoc />
    public void CloseContext() => this.Close(null);

    /// <summary>
    /// Closes the context, optionally recording a given end time on the
    /// underlying X-Ray entity.
    /// </summary>
    /// <param name="endTime">Optional end time to record on the entity, or
    /// <see langword="null"/> to use the SDK's default "now" behaviour.</param>
    internal void Close(DateTime? endTime)
    {
        if (this.entity == null)
        {
            return;
        }

        AWSXRayRecorder recorder = AWSXRayRecorder.Instance;
        try
        {
            if (this.isSegment)
            {
                recorder.EndSegment(endTime);
            }
            else
            {
                recorder.EndSubsegment(endTime);
            }
        }
        catch
        {
            // Telemetry failures must not break database operations.
        }
    }

    private static Entity? SafeGetEntity(AWSXRayRecorder recorder)
    {
        try
        {
            return recorder.GetEntity();
        }
        catch
        {
            return null;
        }
    }
}
