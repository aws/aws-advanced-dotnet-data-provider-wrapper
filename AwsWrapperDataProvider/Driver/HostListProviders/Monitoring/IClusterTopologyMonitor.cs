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

namespace AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;

/// <summary>
/// Interface for monitoring cluster topology changes.
/// </summary>
public interface IClusterTopologyMonitor : IDisposable
{
    /// <summary>
    /// Gets a value indicating whether this monitor can be disposed.
    /// </summary>
    bool CanDispose { get; }

    /// <summary>
    /// Sets the cluster ID for this monitor.
    /// </summary>
    /// <param name="clusterId">The cluster ID to set.</param>
    void SetClusterId(string clusterId);

    /// <summary>
    /// Forces a refresh of the cluster topology.
    /// </summary>
    /// <param name="writerImportant">Whether writer verification is important.</param>
    /// <param name="timeoutMs">Timeout in milliseconds.</param>
    /// <returns>List of host specifications.</returns>
    /// <exception cref="TimeoutException">Thrown when the operation times out.</exception>
    Task<IList<HostSpec>> ForceRefreshAsync(bool writerImportant, long timeoutMs);

    /// <summary>
    /// Forces a refresh of the cluster topology using the provided connection.
    /// </summary>
    /// <param name="connection">The database connection to use.</param>
    /// <param name="timeoutMs">Timeout in milliseconds.</param>
    /// <returns>List of host specifications.</returns>
    /// <exception cref="TimeoutException">Thrown when the operation times out.</exception>
    Task<IList<HostSpec>> ForceRefreshAsync(DbConnection? connection, long timeoutMs);
}
