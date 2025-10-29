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

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySQL;

public class AwsWrapperRelationalConnection : RelationalConnection, IAwsWrapperRelationalConnection
{
    private readonly string wrapperConnectionString;

    public AwsWrapperRelationalConnection(
        RelationalConnectionDependencies dependencies, IRelationalConnection targetRelationalConnection) : base(dependencies)
    {
        this.TargetRelationalConnection = targetRelationalConnection;

        var extension = dependencies.ContextOptions.Extensions
            .OfType<AwsWrapperOptionsExtension>()
            .FirstOrDefault();

        this.wrapperConnectionString = extension?.WrapperConnectionString ?? throw new InvalidOperationException("AwsWrapperOptionsExtension not found.");
    }

    public IRelationalConnection? TargetRelationalConnection { get; set; }

    protected override DbConnection CreateDbConnection()
    {
        return new AwsWrapperConnection(typeof(MySqlConnector.MySqlConnection), this.wrapperConnectionString!);
    }
}
