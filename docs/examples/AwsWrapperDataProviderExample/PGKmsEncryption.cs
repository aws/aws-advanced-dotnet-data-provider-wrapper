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

using AwsWrapperDataProvider;
using AwsWrapperDataProvider.Dialect.Npgsql;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption;
using Npgsql;

namespace AwsWrapperDataProviderExample;

/// <summary>
/// Writes and reads an encrypted column. The value is encrypted before it leaves the application and
/// decrypted on the way back, so the SQL is the same SQL you would write without the plugin.
/// </summary>
/// <remarks>
/// Before running this, the encryption metadata has to exist and the column has to be registered in it -
/// the plugin reads that metadata, it does not create it. The DDL and the registration SQL are in
/// docs/using-the-dotnet-driver/using-plugins/UsingTheKmsEncryptionPlugin.md. The application also needs
/// kms:Decrypt on the master key.
/// </remarks>
public static class PGKmsEncryption
{
    public static async Task Main(string[] args)
    {
        // Load relevant DbConnection dialect
        NpgsqlDialectLoader.Load();

        // The plugin ships in its own NuGet package, so the factory has to be registered before a
        // connection string can name it.
        ConnectionPluginChainBuilder.RegisterPluginFactory<KmsEncryptionPluginFactory>(
            PluginCodes.KmsEncryption);

        const string connectionString =
            "Host=<insert_rds_instance_here>;" +
            "Database=<database_name_here>;" +
            "Username=<username_here>;" +
            "Password=<password_here>;" +
            "KmsRegion=<aws_region_here>;" +
            "Plugins=kmsEncryption;";

        await using AwsWrapperConnection connection =
            new AwsWrapperConnection<NpgsqlConnection>(connectionString);

        try
        {
            await connection.OpenAsync();

            // The value is bound as a parameter, which is what lets the plugin encrypt it. A literal in
            // the SQL text reaches the column readable, and the plugin can only warn about that.
            await using (var insert = connection.CreateCommand())
            {
                insert.CommandText = "INSERT INTO users (id, ssn) VALUES (@id, @ssn)";
                AddParameter(insert, "@id", 1);
                AddParameter(insert, "@ssn", "123-45-6789");

                // Encrypted on the way out.
                await insert.ExecuteNonQueryAsync();
            }

            await using (var select = connection.CreateCommand())
            {
                select.CommandText = "SELECT ssn FROM users WHERE id = @id";
                AddParameter(select, "@id", 1);

                await using var reader = await select.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    // Decrypted on the way back, so this is the original string.
                    Console.WriteLine(reader.GetString(0));
                }
            }

            // Reading the same column without the plugin returns the stored bytes instead. An equality
            // search on an encrypted column never matches, because every value is encrypted with a fresh
            // single-use number - look rows up by a column that is not encrypted, as above.
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
    }

    private static void AddParameter(System.Data.Common.DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
