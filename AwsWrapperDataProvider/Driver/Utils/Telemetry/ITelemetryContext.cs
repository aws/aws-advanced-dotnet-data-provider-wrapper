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

namespace AwsWrapperDataProvider.Driver.Utils.Telemetry;

/// <summary>
/// Represents a single telemetry trace span. Attributes, success status, and
/// exceptions can be recorded on the span before it is closed.
/// </summary>
/// <remarks>
/// <para>
/// This interface intentionally does not implement <see cref="IDisposable"/>:
/// <see cref="SetSuccess"/> and <see cref="SetException"/> must be callable
/// before the span is closed, and the no-op implementation is a shared
/// singleton that should not be disposed.
/// </para>
/// <para>
/// The expected usage is an explicit <c>try</c>/<c>catch</c>/<c>finally</c>
/// block that calls <see cref="CloseContext"/> in the <c>finally</c> branch.
/// </para>
/// </remarks>
public interface ITelemetryContext
{
    /// <summary>
    /// Marks the span as successful or unsuccessful. Should be called before
    /// <see cref="CloseContext"/>.
    /// </summary>
    /// <param name="success"><c>true</c> if the operation succeeded, otherwise
    /// <c>false</c>.</param>
    void SetSuccess(bool success);

    /// <summary>
    /// Sets a string attribute (tag) on the span.
    /// </summary>
    /// <param name="key">The attribute key.</param>
    /// <param name="value">The attribute value.</param>
    void SetAttribute(string key, string value);

    /// <summary>
    /// Records an exception on the span and marks the span status as errored.
    /// </summary>
    /// <param name="exception">The exception to record.</param>
    void SetException(Exception exception);

    /// <summary>
    /// Returns the name of the span.
    /// </summary>
    /// <returns>The name of the span.</returns>
    string GetName();

    /// <summary>
    /// Closes the span and signals that the associated operation has completed.
    /// </summary>
    /// <remarks>
    /// After this method returns, further mutations on the context (for
    /// example <see cref="SetAttribute"/>) are not guaranteed to take effect.
    /// </remarks>
    void CloseContext();
}
