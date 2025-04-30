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
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;

namespace AwsWrapperDataProvider.Driver.Dialects;

/// <summary>
/// Interface for database dialects that define database-specific behavior.
/// </summary>
public interface IDialect
{
    int DefaultPort { get; }

    // TODO: Implement IExceptionHandler
    // IExceptionHandler ExceptionHandler { get; }

    string HostAliasQuery { get; }

    string ServerVersionQuery { get; }

    IList<DialectCodes> DialectUpdateCandidates { get; }

    HostListProviderSupplier HostListProviderSupplier { get; }

    /// <summary>
    /// Determines if the given connection is using this dialect.
    /// </summary>
    /// <param name="conn">The database connection.</param>
    /// <returns>True if the connection is using this dialect, false otherwise.</returns>
    bool IsDialect(DbConnection conn);

    /// <summary>
    /// Used by IConnectionProvider during connection to ensure connectionProps can be used to create a DbConnection.
    /// </summary>
    /// <param name="props">Connection properties.</param>
    /// <param name="hostSpec">HostSpec containing current host information.</param>
    void PrepareConnectionProperties(Dictionary<string, string> props, HostSpec hostSpec);
}

/// <summary>
/// Delegate for creating host list providers.
/// </summary>
/// <param name="props">Connection properties.</param>
/// <param name="hostListProviderService">The host list provider service.</param>
/// <param name="pluginService">The plugin service.</param>
/// <returns>A host list provider.</returns>
public delegate IHostListProvider? HostListProviderSupplier(
    Dictionary<string, string> props,
    IHostListProviderService hostListProviderService,
    IPluginService pluginService);
