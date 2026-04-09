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
using Microsoft.EntityFrameworkCore.Storage;

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL;

public class AwsWrapperRelationalConnection : RelationalConnection, IAwsWrapperRelationalConnection
{
    private readonly string? wrapperConnectionString;

    public AwsWrapperRelationalConnection(
        RelationalConnectionDependencies dependencies, IRelationalConnection targetRelationalConnection) : base(dependencies)
    {
        this.TargetRelationalConnection = targetRelationalConnection;

        // Retrieve the wrapper connection string (with Plugins=... etc.) from our extension.
        var wrapperExtension = dependencies.ContextOptions.FindExtension<AwsWrapperOptionsExtension>();
        this.wrapperConnectionString = wrapperExtension?.WrapperConnectionString;
    }

    public IRelationalConnection? TargetRelationalConnection { get; set; }

    protected override DbConnection CreateDbConnection()
    {
        // Use the wrapper connection string (which includes Plugins= and other wrapper-specific keys)
        // rather than the base ConnectionString (which only has provider-understood keys).
        return new AwsWrapperConnection(typeof(Npgsql.NpgsqlConnection), this.wrapperConnectionString ?? this.ConnectionString!);
    }
}
