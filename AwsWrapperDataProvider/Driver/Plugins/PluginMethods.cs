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

namespace AwsWrapperDataProvider.Driver.Plugins;

public static class PluginMethods
{
    public static IReadOnlySet<string> OpenMethods { get; } = new HashSet<string>()
    {
        "DbConnection.Open",
        "DbConnection.OpenAsync",
    };

    public static IReadOnlySet<string> BeginTransactionMethods { get; } = new HashSet<string>()
    {
        "DbConnection.BeginDbTransaction",
        "DbConnection.BeginDbTransactionAsync",
    };

    public static IReadOnlySet<string> CommandMethods { get; } = new HashSet<string>()
    {
        "DbCommand.ExecuteNonQuery",
        "DbCommand.ExecuteNonQueryAsync",
        "DbCommand.ExecuteReader",
        "DbCommand.ExecuteReaderAsync",
        "DbCommand.ExecuteScalar",
        "DbCommand.ExecuteScalarAsync",
    };

    public static IReadOnlySet<string> BatchMethods { get; } = new HashSet<string>()
    {
        "DbBatch.ExecuteNonQuery",
        "DbBatch.ExecuteNonQueryAsync",
        "DbBatch.ExecuteReader",
        "DbBatch.ExecuteReaderAsync",
        "DbBatch.ExecuteScalar",
        "DbBatch.ExecuteScalarAsync",
    };

    public static IReadOnlySet<string> ReaderMethods { get; } = new HashSet<string>()
    {
        "DbDataReader.Read",
        "DbDataReader.ReadAsync",
        "DbDataReader.NextResult",
        "DbDataReader.NextResultAsync",
    };

    public static IReadOnlySet<string> TransactionMethods { get; } = new HashSet<string>()
    {
        "DbTransaction.Commit",
        "DbTransaction.CommitAsync",
        "DbTransaction.Rollback",
        "DbTransaction.RollbackAsync",
    };

    public static IReadOnlySet<string> SpecialMethods { get; } = new HashSet<string>()
    {
        "initHostProvider",
    };

    public static IReadOnlySet<string> NetworkBoundMethods { get; } =
        new HashSet<string>(OpenMethods
            .Concat(BeginTransactionMethods)
            .Concat(CommandMethods)
            .Concat(BatchMethods)
            .Concat(ReaderMethods)
            .Concat(TransactionMethods));
}
