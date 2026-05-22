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
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Utils.Telemetry;

/// <summary>
/// OTLP-backed <see cref="ITelemetryFactory"/> that uses the built-in .NET
/// diagnostics APIs (<see cref="ActivitySource"/> and <see cref="Meter"/>).
/// The user's application is responsible for configuring the OpenTelemetry
/// SDK with exporters that collect from the source name <c>"aws-advanced-dotnet-wrapper"</c>.
/// </summary>
/// <remarks>
/// <para>
/// The factory is a process-wide singleton. A single <see cref="ActivitySource"/>
/// and <see cref="Meter"/> are shared across all factory operations.
/// </para>
/// <para>
/// Trace-level resolution lives in <see cref="DefaultTelemetryFactory"/>;
/// this factory simply applies whatever level it is given. A
/// <see cref="TelemetryTraceLevel.TopLevel"/> or
/// <see cref="TelemetryTraceLevel.ForceTopLevel"/> request always opens a
/// root span: when an unrelated <see cref="Activity.Current"/> is present
/// the factory detaches it for the lifetime of the root, and the resulting
/// <see cref="OtlpTelemetryContext"/> restores it when closed so that the
/// application's surrounding trace context is preserved across the wrapper
/// call.
/// </para>
/// </remarks>
public sealed class OtlpTelemetryFactory : ITelemetryFactory, ITelemetryParentContextProbe
{
    /// <summary>
    /// Name shared by the <see cref="ActivitySource"/> and the <see cref="Meter"/>.
    /// User applications must configure their OpenTelemetry SDK to listen on
    /// this name.
    /// </summary>
    public const string InstrumentationName = "aws-advanced-dotnet-wrapper";

    private const string CopyPrefix = "copy: ";

    private static readonly ILogger<OtlpTelemetryFactory> Logger =
        LoggerUtils.GetLogger<OtlpTelemetryFactory>();

    private static readonly ActivitySource TelemetryActivitySource = new(InstrumentationName);
    private static readonly Meter TelemetryMeter = new(InstrumentationName);

    /// <summary>
    /// Shared singleton instance.
    /// </summary>
    public static readonly OtlpTelemetryFactory Instance = new();

    private OtlpTelemetryFactory()
    {
    }

    /// <inheritdoc />
    public bool HasParentContext() => Activity.Current != null;

    /// <inheritdoc />
    public ITelemetryContext OpenTelemetryContext(string name, TelemetryTraceLevel traceLevel)
    {
        switch (traceLevel)
        {
            case TelemetryTraceLevel.ForceTopLevel:
            case TelemetryTraceLevel.TopLevel:
                return StartRoot(name);

            case TelemetryTraceLevel.Nested:
                Activity? nestedActivity = TelemetryActivitySource.StartActivity(name, ActivityKind.Internal);
                return nestedActivity != null
                    ? new OtlpTelemetryContext(nestedActivity, name)
                    : NullTelemetryContext.Instance;

            case TelemetryTraceLevel.NoTrace:
            default:
                return NullTelemetryContext.Instance;
        }
    }

    /// <inheritdoc />
    public void PostCopy(ITelemetryContext context, TelemetryTraceLevel traceLevel)
    {
        // Nothing to copy if the original was already a no-op.
        if (context is not OtlpTelemetryContext otlpContext)
        {
            return;
        }

        string copyName = CopyPrefix + context.GetName();
        DateTime startTime = otlpContext.StartTime;
        DateTime endTime = otlpContext.EndTime;

        // Run on a thread-pool thread and explicitly null Activity.Current
        // inside the lambda so the copied Activity is created as an
        // independent root span. Task.Run alone is not sufficient: it flows
        // the calling ExecutionContext (and therefore Activity.Current) into
        // the lambda, so without the explicit detach the copy would inherit
        // any outer wrapper span still open on the caller and end up in the
        // same trace as the original. Mirrors AWSXRayRecorder.ClearEntity()
        // in XRayTelemetryFactory.PostCopy. The lambda's mutation of
        // Activity.Current is scoped to the task's logical context and does
        // not affect the calling thread.
        Task.Run(() =>
        {
            Activity.Current = null;

            Activity? copyActivity = TelemetryActivitySource.StartActivity(copyName, ActivityKind.Client);
            if (copyActivity == null)
            {
                return;
            }

            otlpContext.CopyAttributesTo(copyActivity);
            copyActivity.SetStartTime(startTime);
            copyActivity.SetEndTime(endTime);
            copyActivity.Stop();
        }).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public ITelemetryCounter CreateCounter(string name)
    {
        try
        {
            Counter<long> counter = TelemetryMeter.CreateCounter<long>(name);
            return new OtlpTelemetryCounter(counter);
        }
        catch (Exception ex)
        {
            // Telemetry failures must not break database operations; fall
            // back to a no-op counter.
            Logger.LogDebug(
                ex,
                "Failed to create OTLP counter '{CounterName}'; falling back to NullTelemetryCounter.",
                name);
            return NullTelemetryCounter.Instance;
        }
    }

    /// <inheritdoc />
    public ITelemetryGauge CreateGauge(string name, Func<long> valueCallback)
    {
        try
        {
            // The created ObservableGauge is kept alive by the Meter's
            // internal instrument list; we do not need to retain a reference.
            TelemetryMeter.CreateObservableGauge(name, valueCallback);
            return new OtlpTelemetryGauge();
        }
        catch (Exception ex)
        {
            Logger.LogDebug(
                ex,
                "Failed to create OTLP gauge '{GaugeName}'; falling back to NullTelemetryGauge.",
                name);
            return NullTelemetryGauge.Instance;
        }
    }

    /// <summary>
    /// Opens a root Activity, detaching any pre-existing
    /// <see cref="Activity.Current"/> for the duration of the new span and
    /// arranging for the original to be restored when the span closes.
    /// </summary>
    /// <param name="name">The span name.</param>
    /// <returns>An <see cref="ITelemetryContext"/> wrapping the root
    /// Activity, or <see cref="NullTelemetryContext.Instance"/> when no
    /// <see cref="ActivityListener"/> is sampling the source.</returns>
    private static ITelemetryContext StartRoot(string name)
    {
        // Capture the application's current Activity so it can be re-attached
        // after the wrapper's root span ends. Without this, when the new root
        // closes Activity.Current would revert to whatever the root's parent
        // is (null, since we cleared it), and the application would lose its
        // surrounding trace context for any spans it opens after the wrapper
        // call returns.
        Activity? saved = Activity.Current;
        if (saved != null)
        {
            Activity.Current = null;
        }

        Activity? rootActivity = TelemetryActivitySource.StartActivity(name, ActivityKind.Client);
        if (rootActivity == null)
        {
            // No listener is sampling: restore the saved parent immediately
            // and return a no-op context so callers see a uniform shape.
            if (saved != null)
            {
                Activity.Current = saved;
            }

            return NullTelemetryContext.Instance;
        }

        // Leave the new root as Activity.Current so that nested wrapper
        // spans created during the call (plugin chain, target driver call,
        // etc.) attach to it rather than to the application's parent.
        return new OtlpTelemetryContext(rootActivity, name, restoreOnClose: saved);
    }
}
