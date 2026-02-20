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

namespace AwsWrapperDataProvider;

/// <summary>
/// Interface for provider classes that wrap a delegate instance
/// and allow access to the underlying implementation.
/// </summary>
public interface IWrapper
{
    /// <summary>
    /// Returns an object that implements the specified interface to allow access to non-standard or provider-specific methods.
    /// </summary>
    /// <typeparam name="T">The interface type to unwrap to.</typeparam>
    /// <returns> An object implementing <typeparamref name="T"/> May be a proxy for the underlying implementation.</returns>
    T Unwrap<T>() where T : class;

    /// <summary>
    /// Returns true if this instance implements the specified interface or wraps an object that does.
    /// </summary>
    /// <typeparam name="T"> The interface type to test.</typeparam>
    /// <returns>True if this implements or wraps an object implementing <typeparamref name="T"/>.</returns>
    bool IsWrapperFor<T>() where T : class;
}
