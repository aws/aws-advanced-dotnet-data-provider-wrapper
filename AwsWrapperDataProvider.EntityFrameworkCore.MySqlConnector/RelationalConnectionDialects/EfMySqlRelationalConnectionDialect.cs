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
using MySqlConnector;

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector.RelationalConnectionDialects;

/// <summary>
/// Dialect for the Entity Framework Core MySQL providers built on MySqlConnector: the connection is a
/// <see cref="MySqlConnection"/>, and the options those providers require are applied to the wrapper's
/// connection string.
/// </summary>
/// <remarks>
/// Not named after a specific provider because neither responsibility is provider-specific. The wrapper
/// creates the connection itself rather than letting the provider build one, so whatever the provider
/// would apply to its own connection string never runs; this type has to apply the mandatory options in
/// its place. Both options follow from how Entity Framework Core talks to MySqlConnector, so they hold
/// for any provider in this family: <c>AllowUserVariables</c> because the generated SQL uses session
/// user variables, and <c>UseAffectedRows=false</c> because concurrency handling needs rows matched
/// rather than rows changed.
/// </remarks>
internal sealed class EfMySqlRelationalConnectionDialect : IRelationalConnectionDialect
{
    internal static readonly EfMySqlRelationalConnectionDialect Instance = new();

    private EfMySqlRelationalConnectionDialect()
    {
    }

    /// <inheritdoc />
    public Type UnderlyingConnectionType => typeof(MySqlConnection);

    /// <inheritdoc />
    public string NormalizeConnectionString(string wrapperConnectionString)
    {
        var cs = wrapperConnectionString;
        ApplyMandatoryMySqlConnectorOptions(ref cs);
        return cs;
    }

    private static void ApplyMandatoryMySqlConnectorOptions(ref string connectionString)
    {
        var builder = new DbConnectionStringBuilder
        {
            ConnectionString = connectionString,
        };

        builder["AllowUserVariables"] = true;
        builder["UseAffectedRows"] = false;
        connectionString = builder.ConnectionString;
    }
}
