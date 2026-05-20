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
/// X-Ray-backed <see cref="ITelemetryFactory"/>. Produces
/// <see cref="XRayTelemetryContext"/> instances for trace operations and
/// throws <see cref="NotSupportedException"/> for metric operations as
/// AWS X-Ray does not support metrics.
/// </summary>
/// <remarks>
/// <para>
/// Consumers register this factory under the <c>"XRAY"</c> backend name via
/// <see cref="XRayTelemetryLoader.Load"/>. Once registered, configuring
/// <c>TelemetryTracesBackend=XRAY</c> routes trace contexts through this
/// factory; metric calls are routed separately (typically to OTLP or Null).
/// </para>
/// <para>
/// Trace-level resolution lives in <see cref="DefaultTelemetryFactory"/>;
/// this factory simply applies whatever level it is given. The X-Ray SDK's
/// <c>BeginSegment</c> always starts a new top-level segment, and
/// <c>EndSegment</c> / <c>EndSubsegment</c> already restore the parent
/// entity, so no manual save/restore bookkeeping is required here.
/// </para>
/// </remarks>
public sealed class XRayTelemetryFactory : ITelemetryFactory, ITelemetryParentContextProbe
{
    private const string CopyPrefix = "copy: ";
    private const string MetricsNotSupportedMessage =
        "AWS X-Ray does not support metrics. Configure a separate metrics backend "
        + "(for example TelemetryMetricsBackend=OTLP).";

    /// <inheritdoc />
    public bool HasParentContext() => AWSXRayRecorder.Instance.IsEntityPresent();

    /// <inheritdoc />
    public ITelemetryContext OpenTelemetryContext(string name, TelemetryTraceLevel traceLevel)
        => new XRayTelemetryContext(name, traceLevel);

    /// <inheritdoc />
    public void PostCopy(ITelemetryContext context, TelemetryTraceLevel traceLevel)
    {
        if (context is not XRayTelemetryContext source)
        {
            // Mismatched type — silently ignore per the "telemetry must not
            // break database operations" policy.
            return;
        }

        if (traceLevel == TelemetryTraceLevel.NoTrace)
        {
            return;
        }

        Entity? sourceEntity = source.Entity;
        if (sourceEntity == null)
        {
            // Source was a no-op; there is nothing to copy.
            return;
        }

        CloneState state = CloneState.From(source, sourceEntity);

        if (traceLevel is TelemetryTraceLevel.ForceTopLevel or TelemetryTraceLevel.TopLevel)
        {
            // Run on a thread-pool thread with the X-Ray trace context
            // cleared so the clone is an independent root segment.
            Task.Run(() =>
            {
                AWSXRayRecorder.Instance.ClearEntity();
                EmitClone(state, traceLevel);
            }).GetAwaiter().GetResult();
        }
        else
        {
            // Nested clone — inline, attaches to the current parent.
            EmitClone(state, traceLevel);
        }
    }

    /// <inheritdoc />
    public ITelemetryCounter CreateCounter(string name)
        => throw new NotSupportedException(MetricsNotSupportedMessage);

    /// <inheritdoc />
    public ITelemetryGauge CreateGauge(string name, Func<long> valueCallback)
        => throw new NotSupportedException(MetricsNotSupportedMessage);

    private static void EmitClone(CloneState state, TelemetryTraceLevel traceLevel)
    {
        XRayTelemetryContext clone = new(CopyPrefix + state.Name, traceLevel);
        Entity? cloneEntity = clone.Entity;
        if (cloneEntity == null)
        {
            // BeginSegment / BeginSubsegment declined (for example, Nested
            // with no parent on the clean task thread). Nothing to emit.
            return;
        }

        // Copy every source annotation except the trace name, which the
        // clone has already set to "copy: <name>".
        foreach (KeyValuePair<string, object> kvp in state.Annotations)
        {
            if (kvp.Key == XRayTelemetryContext.TraceNameAnnotation || kvp.Value == null)
            {
                continue;
            }

            clone.SetAttribute(kvp.Key, kvp.Value.ToString()!);
        }

        // Record the source trace id for cross-trace correlation.
        if (!string.IsNullOrEmpty(state.TraceId))
        {
            clone.SetAttribute(XRayTelemetryContext.SourceTraceAnnotation, state.TraceId);
        }

        // Mirror the source's error / fault flags.
        cloneEntity.HasError = state.HasError;
        cloneEntity.HasFault = state.HasFault;

        // Overwrite the clone's start time (which BeginSegment set to "now")
        // with the source's start time.
        cloneEntity.StartTime = state.StartTime;

        // Close the clone, passing the source's end time so EndSegment /
        // EndSubsegment does not overwrite it with "now".
        clone.Close(UnixSecondsToDateTime(state.EndTime));
    }

    private static DateTime UnixSecondsToDateTime(decimal unixSeconds)
    {
        // Convert the decimal Unix seconds to DateTime with millisecond
        // precision. Sub-millisecond precision is lost in this round trip,
        // but that is acceptable for post-hoc trace copies.
        long ms = (long)Math.Round(unixSeconds * 1000m);
        return DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;
    }

    /// <summary>
    /// Immutable snapshot of the source context's state captured at
    /// <c>PostCopy</c> call time.
    /// </summary>
    private readonly struct CloneState
    {
        public string Name { get; }

        public string TraceId { get; }

        public decimal StartTime { get; }

        public decimal EndTime { get; }

        public bool HasError { get; }

        public bool HasFault { get; }

        public IReadOnlyList<KeyValuePair<string, object>> Annotations { get; }

        private CloneState(
            string name,
            string traceId,
            decimal startTime,
            decimal endTime,
            bool hasError,
            bool hasFault,
            IReadOnlyList<KeyValuePair<string, object>> annotations)
        {
            this.Name = name;
            this.TraceId = traceId;
            this.StartTime = startTime;
            this.EndTime = endTime;
            this.HasError = hasError;
            this.HasFault = hasFault;
            this.Annotations = annotations;
        }

        public static CloneState From(XRayTelemetryContext source, Entity sourceEntity)
        {
            // Materialize the annotations collection so later iteration on
            // the clone thread is stable even if the source entity is
            // mutated concurrently.
            List<KeyValuePair<string, object>> annotations = new();
            foreach (KeyValuePair<string, object> kvp in sourceEntity.Annotations)
            {
                annotations.Add(kvp);
            }

            return new CloneState(
                source.GetName(),
                sourceEntity.TraceId ?? string.Empty,
                sourceEntity.StartTime,
                sourceEntity.EndTime,
                sourceEntity.HasError,
                sourceEntity.HasFault,
                annotations);
        }
    }
}
