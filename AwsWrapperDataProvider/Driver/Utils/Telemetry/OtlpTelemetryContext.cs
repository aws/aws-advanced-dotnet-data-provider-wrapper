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

namespace AwsWrapperDataProvider.Driver.Utils.Telemetry;

/// <summary>
/// OTLP-backed <see cref="ITelemetryContext"/> implementation that wraps a
/// <see cref="Activity"/> from <c>System.Diagnostics</c>. Attributes and
/// exceptions are recorded on the underlying Activity using OpenTelemetry
/// semantic conventions; exporters in the user's application then ship the
/// spans via OTLP.
/// </summary>
public sealed class OtlpTelemetryContext : ITelemetryContext
{
    /// <summary>Attribute key used by <see cref="SetSuccess"/>.</summary>
    internal const string SuccessTagName = "success";

    /// <summary>Event name for recorded exceptions (OpenTelemetry convention).</summary>
    internal const string ExceptionEventName = "exception";

    private readonly Activity activity;
    private readonly string name;
    private readonly Activity? restoreOnClose;

    /// <summary>
    /// Initializes a new instance of the <see cref="OtlpTelemetryContext"/>
    /// class wrapping the specified <see cref="Activity"/>.
    /// </summary>
    /// <param name="activity">The underlying Activity. Must already be
    /// started by the caller (for example, via
    /// <see cref="ActivitySource.StartActivity(string, ActivityKind)"/>).</param>
    /// <param name="name">The span name to return from <see cref="GetName"/>.</param>
    public OtlpTelemetryContext(Activity activity, string name)
        : this(activity, name, restoreOnClose: null)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="OtlpTelemetryContext"/>
    /// class wrapping the specified <see cref="Activity"/>, optionally
    /// restoring a previous <see cref="Activity.Current"/> when this context
    /// is closed.
    /// </summary>
    /// <param name="activity">The underlying Activity. Must already be
    /// started.</param>
    /// <param name="name">The span name to return from <see cref="GetName"/>.</param>
    /// <param name="restoreOnClose">An <see cref="Activity"/> to assign back
    /// to <see cref="Activity.Current"/> in <see cref="CloseContext"/>. Used
    /// by <see cref="OtlpTelemetryFactory"/> when a TopLevel span is opened
    /// while an unrelated parent Activity exists on the calling logical
    /// thread: the parent is detached so the new Activity is a true root
    /// and is restored when the root closes so the application's trace
    /// context is preserved across the wrapper call. Pass
    /// <see langword="null"/> when no restore is needed.</param>
    public OtlpTelemetryContext(Activity activity, string name, Activity? restoreOnClose)
    {
        this.activity = activity;
        this.name = name;
        this.restoreOnClose = restoreOnClose;
    }

    /// <summary>
    /// Gets the start time of the underlying Activity, in UTC.
    /// </summary>
    public DateTime StartTime => this.activity.StartTimeUtc;

    /// <summary>
    /// Gets the end time of the underlying Activity, computed as
    /// <c>StartTimeUtc + Duration</c>. Only meaningful after
    /// <see cref="CloseContext"/> has been called (Activity.Stop sets Duration).
    /// </summary>
    public DateTime EndTime => this.activity.StartTimeUtc + this.activity.Duration;

    /// <inheritdoc />
    public void SetSuccess(bool success)
    {
        this.activity.SetTag(SuccessTagName, success);
    }

    /// <inheritdoc />
    public void SetAttribute(string key, string value)
    {
        this.activity.SetTag(key, value);
    }

    /// <inheritdoc />
    public void SetException(Exception exception)
    {
        ActivityTagsCollection tags = new()
        {
            { "exception.type", exception.GetType().FullName ?? exception.GetType().Name },
            { "exception.message", exception.Message },
            { "exception.stacktrace", exception.ToString() },
        };

        this.activity.AddEvent(new ActivityEvent(ExceptionEventName, DateTimeOffset.UtcNow, tags));
        this.activity.SetStatus(ActivityStatusCode.Error, exception.Message);
    }

    /// <inheritdoc />
    public string GetName() => this.name;

    /// <inheritdoc />
    public void CloseContext()
    {
        this.activity.Stop();

        // Restore the application's parent Activity that was detached when
        // this root span was opened. Without this, Activity.Current would
        // revert to whatever the new root's parent is (typically null), and
        // any spans the application opens after the wrapper call returns
        // would lose their original trace context.
        if (this.restoreOnClose != null)
        {
            Activity.Current = this.restoreOnClose;
        }
    }

    /// <summary>
    /// Copies every tag from the wrapped Activity onto the specified target
    /// Activity.
    /// </summary>
    /// <param name="target">The Activity to copy tags onto.</param>
    public void CopyAttributesTo(Activity target)
    {
        foreach (KeyValuePair<string, object?> tag in this.activity.TagObjects)
        {
            target.SetTag(tag.Key, tag.Value);
        }
    }
}
