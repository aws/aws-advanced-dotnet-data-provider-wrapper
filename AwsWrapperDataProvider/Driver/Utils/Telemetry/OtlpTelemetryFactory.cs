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
/// </remarks>
public sealed class OtlpTelemetryFactory : ITelemetryFactory
{
    /// <summary>
    /// Name shared by the <see cref="ActivitySource"/> and the <see cref="Meter"/>.
    /// User applications must configure their OpenTelemetry SDK to listen on
    /// this name.
    /// </summary>
    public const string InstrumentationName = "aws-advanced-dotnet-wrapper";

    private const string CopyPrefix = "copy: ";

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
    public ITelemetryContext OpenTelemetryContext(string name, TelemetryTraceLevel traceLevel)
    {
        switch (traceLevel)
        {
            case TelemetryTraceLevel.ForceTopLevel:
            case TelemetryTraceLevel.TopLevel:
                // TopLevel/ForceTopLevel only reach this factory on code paths
                // where Activity.Current is expected to be null (either
                // DefaultTelemetryFactory converts TopLevel to Nested when a
                // parent is present, or ForceTopLevel is called from
                // PostCopy's Task.Run on a clean thread). The defensive guard
                // returns a no-op if the expectation is violated.
                if (Activity.Current != null)
                {
                    return NullTelemetryContext.Instance;
                }

                Activity? rootActivity = TelemetryActivitySource.StartActivity(name, ActivityKind.Client);
                return rootActivity != null
                    ? new OtlpTelemetryContext(rootActivity, name)
                    : NullTelemetryContext.Instance;

            case TelemetryTraceLevel.Nested:
                // A nested span must attach to an existing parent.
                if (Activity.Current == null)
                {
                    return NullTelemetryContext.Instance;
                }

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

        // Run on a thread-pool thread where Activity.Current is null so that
        // the copied Activity is created as an independent root span,
        // regardless of any parent Activity on the calling thread.
        Task.Run(() =>
        {
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
        catch
        {
            // Telemetry failures must not break database operations; fall
            // back to a no-op counter.
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
        catch
        {
            return NullTelemetryGauge.Instance;
        }
    }
}
