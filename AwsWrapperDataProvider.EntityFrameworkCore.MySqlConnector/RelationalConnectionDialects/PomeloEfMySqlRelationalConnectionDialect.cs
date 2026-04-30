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
using System.Reflection;
using MySqlConnector;

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector.RelationalConnectionDialects;

/// <summary>
/// Pomelo <c>EntityFrameworkCore.MySql</c>: targets <see cref="MySqlConnection"/>. Normalization invokes Pomelo's internal
/// mandatory connection-string options when present; otherwise applies the same MySqlConnector defaults manually.
/// </summary>
internal sealed class PomeloEfMySqlRelationalConnectionDialect : IRelationalConnectionDialect
{
    internal static readonly PomeloEfMySqlRelationalConnectionDialect Instance = new();

    private PomeloEfMySqlRelationalConnectionDialect()
    {
    }

    /// <inheritdoc />
    public Type UnderlyingConnectionType => typeof(MySqlConnection);

    /// <inheritdoc />
    public string NormalizeConnectionString(string wrapperConnectionString)
    {
        var cs = wrapperConnectionString;
        ApplyManualMySqlConnectorMandatoryDefaults(ref cs);
        return cs;
    }

    private static void ApplyManualMySqlConnectorMandatoryDefaults(ref string connectionString)
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
