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

using System.Data;
using AwsWrapperDataProvider;
using AwsWrapperDataProvider.Dialect.Npgsql;
using AwsWrapperDataProvider.Driver.Plugins.ReadWriteSplitting;
using Npgsql;

namespace AwsWrapperDataProviderExample;

/// <summary>
/// Demonstrates the Read/Write Splitting plugin with Aurora PostgreSQL:
/// connect to the writer, perform a write, switch to a reader for a read, then switch back to the writer.
/// </summary>
public static class PGReadWriteSplitting
{
    public static async Task Main(string[] args)
    {
        NpgsqlDialectLoader.Load();

        const string connectionString =
            "Host=<cluster-endpoint>;Database=<db_name>;Username=<username>;Password=<password>;" +
            "Plugins=failover,efm,readWriteSplitting";

        try
        {
            await using var connection = new AwsWrapperConnection<NpgsqlConnection>(connectionString);
            await connection.OpenAsync();

            // Write on writer (default after open)
            await using (var cmd = connection.CreateCommand<NpgsqlCommand>())
            {
                cmd.CommandText = "INSERT INTO items (id, name) VALUES (1, 'Example')";
                await cmd.ExecuteNonQueryAsync();
            }

            // Switch to reader and read
            await using (var cmd = connection.CreateCommand<NpgsqlCommand>())
            {
                cmd.CommandText = "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY";
                await cmd.ExecuteNonQueryAsync();
            }

            await using (var cmd = connection.CreateCommand<NpgsqlCommand>())
            {
                cmd.CommandText = "SELECT COUNT(*) FROM items";
                var count = await cmd.ExecuteScalarAsync();
                Console.WriteLine($"Count: {count}");
            }

            // Switch back to writer
            await using (var cmd = connection.CreateCommand<NpgsqlCommand>())
            {
                cmd.CommandText = "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE";
                await cmd.ExecuteNonQueryAsync();
            }
        }
        catch (ReadWriteSplittingDbException ex)
        {
            Console.WriteLine($"Read/Write Splitting error: {ex.Message}");
            throw;
        }
    }
}
