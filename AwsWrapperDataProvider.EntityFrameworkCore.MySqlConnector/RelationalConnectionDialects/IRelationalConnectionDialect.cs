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

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector.RelationalConnectionDialects;

/// <summary>
/// Per–EF-MySQL-provider rules for the wrapper relational connection (underlying ADO.NET type and wrapper connection-string normalization).
/// </summary>
public interface IRelationalConnectionDialect
{
    /// <summary>
    /// Gets the target connection type passed to <see cref="AwsWrapperDataProvider.AwsWrapperConnection"/>.
    /// </summary>
    Type UnderlyingConnectionType { get; }

    /// <summary>
    /// Normalizes the user-supplied wrapper connection string before opening the wrapper connection.
    /// </summary>
    /// <param name="wrapperConnectionString">Connection string including wrapper keys (e.g. <c>Plugins=</c>).</param>
    /// <returns>Connection string to store on the opened <see cref="AwsWrapperDataProvider.AwsWrapperConnection"/>.</returns>
    string NormalizeConnectionString(string wrapperConnectionString);
}
