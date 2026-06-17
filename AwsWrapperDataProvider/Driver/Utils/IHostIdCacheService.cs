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

using System.Data.Common;
using AwsWrapperDataProvider.Driver.HostInfo;

namespace AwsWrapperDataProvider.Driver.Utils;

/// <summary>
/// Caches the result of identifying a connection keyed by the connection host name.
/// This avoids querying the database again when subsequent calls identify a connection for the same host.
/// </summary>
public interface IHostIdCacheService
{
    /// <summary>
    /// Identifies the connected host, using the cache when eligible.
    /// </summary>
    /// <param name="connection">The connection to be identified.</param>
    /// <param name="connectionHostSpec">The host specification of the provided connection.</param>
    /// <param name="pluginService">The plugin service instance.</param>
    /// <param name="transaction">The database transaction.</param>
    /// <returns>The identified host specification for the connection, or null if it cannot be identified.</returns>
    Task<HostSpec?> IdentifyConnectionAsync(
        DbConnection connection,
        HostSpec connectionHostSpec,
        IPluginService pluginService,
        DbTransaction? transaction = null);
}
