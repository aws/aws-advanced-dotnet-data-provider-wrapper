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

namespace AwsWrapperDataProvider.Driver.Auth;

/// <summary>
/// Returns the current password/token for a target connection.
/// <para>
/// Invoked by the target driver (or its data source) whenever it opens a new physical connection,
/// so the delegate must be safe to call at any time — potentially long after the originating
/// <c>Open()</c> call has returned — and should return quickly. Implementations are expected to
/// satisfy the common path from an in-memory cache and only perform network I/O on a cache miss.
/// </para>
/// </summary>
/// <param name="cancellationToken">A token observed for cancellation of the password fetch.</param>
/// <returns>The current password or authentication token.</returns>
public delegate ValueTask<string> WrapperPasswordProvider(CancellationToken cancellationToken);
