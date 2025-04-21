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
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.TargetDriverDialects;

namespace AwsWrapperDataProvider.Driver.ConnectionProviders;

/// <summary>
/// Interface for connection providers that handle the physical connection creation process.
/// </summary>
public interface IConnectionProvider
{
    /// <summary>
    /// Indicates whether this ConnectionProvider can provide connections for the given host and properties.
    /// </summary>
    /// <param name="hostSpec">The HostSpec containing the host-port information for the host to connect to.</param>
    /// <param name="props">The properties to use for the connection.</param>
    /// <returns>True if this ConnectionProvider can provide connections for the given URL, otherwise false.</returns>
    bool AcceptsUrl(HostSpec hostSpec, Dictionary<string, string> props);

    /// <summary>
    /// Called once per connection that needs to be created.
    /// </summary>
    /// <param name="dialect">The database dialect.</param>
    /// <param name="targetDriverDialect">The target driver dialect.</param>
    /// <param name="hostSpec">The HostSpec containing the host-port information for the host to connect to.</param>
    /// <param name="props">The properties to use for the connection.</param>
    /// <returns>Connection resulting from the given connection information.</returns>
    DbConnection Connect(IDialect dialect, ITargetDriverDialect targetDriverDialect, HostSpec hostSpec,
        Dictionary<string, string> props);

    /// <summary>
    /// Gets the name of the target driver.
    /// </summary>
    /// <returns>The name of the target driver.</returns>
    string GetTargetName();
}
