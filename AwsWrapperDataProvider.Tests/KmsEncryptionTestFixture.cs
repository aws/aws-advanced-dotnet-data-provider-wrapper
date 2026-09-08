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
using System.Security.Cryptography;
using Amazon;
using Amazon.KeyManagementService;
using Amazon.KeyManagementService.Model;
using AwsWrapperDataProvider.Tests.Container;
using AwsWrapperDataProvider.Tests.Container.Utils;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

/// <summary>
/// Creates the encryption schema once for <see cref="KmsEncryptionTests"/>, and removes it again afterwards.
/// </summary>
/// <remarks>
/// <para>
/// The schema is deliberately the same one the AWS Advanced JDBC Wrapper's <c>KmsEncryptionIntegrationTest</c>
/// builds: <c>users(id, name, ssn, email)</c> with <c>ssn</c> registered for encryption, the metadata tables
/// in <c>encrypt</c>, and on PostgreSQL the <c>encrypted_data</c> domain plus the
/// <c>validate_encrypted_data_hmac</c> trigger. Matching it means the two suites are comparable case by case,
/// and that a row written here is one the other driver's setup would accept.
/// </para>
/// <para>
/// The PostgreSQL trigger is the part worth knowing about: it recomputes each value's HMAC with pgcrypto as
/// the row goes in, so every write in the suite is verified by the server independently of the code under
/// test. A plaintext, or a value whose signature does not match, is rejected by the database rather than
/// being noticed later on read. MySQL has no equivalent here, which is a real difference in protection
/// rather than an omission - see the plugin documentation.
/// </para>
/// <para>
/// Does nothing when <see cref="KeyArnVariable"/> is unset, so an environment without a key still runs to
/// completion with the tests skipping.
/// </para>
/// <para>
/// The data key is generated here so the fixture owns its own key material. Only the encrypted form is
/// stored; the plaintext is discarded with the response and never written or logged.
/// </para>
/// </remarks>
public sealed class KmsEncryptionTestFixture : IAsyncLifetime
{
    /// <summary>The default of <c>KmsEncryptionMetadataSchema</c>, which is what the plugin looks in.</summary>
    internal const string MetadataSchema = "encrypt";

    internal const string TableName = "users";
    internal const string EncryptedColumn = "ssn";
    internal const string KeyName = "test-key-users-ssn";
    internal const string Algorithm = "AES-256-GCM";

    /// <summary>The PostgreSQL domain the encrypted column is declared as.</summary>
    internal const string DomainName = "encrypted_data";

    /// <summary>The leading signature over the rest of a stored value.</summary>
    internal const int SignatureLength = 32;

    /// <summary>The shortest signed region there can be: type marker, nonce and cipher tag.</summary>
    internal const int MinimumSignedLength = 1 + 12 + 16;

    /// <summary>
    /// The environment variable holding the KMS key to encrypt with.
    /// </summary>
    /// <remarks>
    /// Named to match what the other drivers' integration tests read, so one variable serves them all.
    /// </remarks>
    internal const string KeyArnVariable = "AWS_KMS_KEY_ARN";

    /// <summary>Why every test in the class skips when no key is configured.</summary>
    internal const string NoKeyReason = KeyArnVariable + " is not set.";

    internal static string? KeyArn => Environment.GetEnvironmentVariable(KeyArnVariable);

    /// <summary>Gets a value indicating whether a key is configured and the schema was created.</summary>
    internal bool Enabled { get; private set; }

    internal static bool IsMySql => IntegrationTestBase.Engine == DatabaseEngine.MYSQL;

    public async ValueTask InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(KeyArn))
        {
            return;
        }

        using var kms = new AmazonKeyManagementServiceClient(
            RegionEndpoint.GetBySystemName(TestEnvironment.Env.Info.Region));

        GenerateDataKeyResponse dataKey = await kms.GenerateDataKeyAsync(
            new GenerateDataKeyRequest { KeyId = KeyArn, KeySpec = DataKeySpec.AES_256 });

        string encryptedDataKey = Convert.ToBase64String(dataKey.CiphertextBlob.ToArray());
        byte[] hmacKey = RandomNumberGenerator.GetBytes(32);

        // A plugin-free connection, so the whole schema is in place before anything reads it.
        await using DbConnection plain = await OpenPlainAsync();
        await DropAsync(plain);

        await ExecuteAsync(plain, $"CREATE SCHEMA {MetadataSchema}");
        await CreateMetadataTablesAsync(plain);
        await InsertKeyAndMetadataAsync(plain, dataKey.KeyId, encryptedDataKey, hmacKey);

        if (IsMySql)
        {
            await ExecuteAsync(plain, $"CREATE TABLE {TableName} ("
                + "id INT AUTO_INCREMENT PRIMARY KEY, "
                + "name VARCHAR(100), "
                + $"{EncryptedColumn} VARBINARY(256), "
                + "email VARCHAR(100))");
        }
        else
        {
            await CreateEncryptedDataTypeAsync(plain);
            await ExecuteAsync(plain, $"CREATE TABLE {TableName} ("
                + "id SERIAL PRIMARY KEY, "
                + "name VARCHAR(100), "
                + $"{EncryptedColumn} {DomainName}, "
                + "email VARCHAR(100))");

            // Every write to the column is verified by the server from here on.
            await ExecuteAsync(
                plain,
                $"CREATE TRIGGER validate_{EncryptedColumn}_hmac "
                + $"BEFORE INSERT OR UPDATE ON {TableName} "
                + $"FOR EACH ROW EXECUTE FUNCTION validate_encrypted_data_hmac('{EncryptedColumn}')");
        }

        await VerifyRegistrationAsync(plain);
        this.Enabled = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (!this.Enabled)
        {
            return;
        }

        try
        {
            await using DbConnection plain = await OpenPlainAsync();
            await DropAsync(plain);
        }
        catch (Exception ex)
        {
            // Teardown must not fail the run; the objects are named distinctly enough to find and remove.
            Console.WriteLine($"Could not drop the kmsEncryption test schema: {ex.Message}");
        }
    }

    /// <summary>Opens a connection with no plugin, used for setup and for reading stored bytes.</summary>
    internal static async Task<DbConnection> OpenPlainAsync()
    {
        string connectionString = ConnectionStringHelper.GetUrl(
            IntegrationTestBase.Engine,
            IntegrationTestBase.Endpoint,
            IntegrationTestBase.Port,
            IntegrationTestBase.Username,
            IntegrationTestBase.Password,
            IntegrationTestBase.DefaultDbName,
            enablePooling: false);

        DbConnection connection = IsMySql
            ? new MySqlConnection(connectionString)
            : new NpgsqlConnection(connectionString);

        await connection.OpenAsync();
        return connection;
    }

    private static async Task ExecuteAsync(DbConnection connection, string sql)
    {
        await using DbCommand command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task CreateMetadataTablesAsync(DbConnection plain)
    {
        string serial = IsMySql ? "INT AUTO_INCREMENT" : "SERIAL";
        string bytes = IsMySql ? "VARBINARY(32)" : "BYTEA";
        string stamp = IsMySql ? "TIMESTAMP" : "TIMESTAMPTZ";
        string onUpdate = IsMySql ? " ON UPDATE CURRENT_TIMESTAMP" : string.Empty;

        await ExecuteAsync(plain, $"CREATE TABLE {MetadataSchema}.key_storage ("
            + $"id {serial} PRIMARY KEY, "
            + "name VARCHAR(255) NOT NULL, "
            + "master_key_arn VARCHAR(512) NOT NULL, "
            + "encrypted_data_key TEXT NOT NULL, "
            + $"hmac_key {bytes} NOT NULL, "
            + "key_spec VARCHAR(50) DEFAULT 'AES_256', "
            + $"created_at {stamp} DEFAULT CURRENT_TIMESTAMP, "
            + $"last_used_at {stamp} DEFAULT CURRENT_TIMESTAMP)");

        await ExecuteAsync(plain, $"CREATE TABLE {MetadataSchema}.encryption_metadata ("
            + "table_name VARCHAR(255) NOT NULL, "
            + "column_name VARCHAR(255) NOT NULL, "
            + "encryption_algorithm VARCHAR(50) NOT NULL, "
            + $"key_id {(IsMySql ? "INT" : "INTEGER")} NOT NULL, "
            + $"created_at {stamp} DEFAULT CURRENT_TIMESTAMP, "
            + $"updated_at {stamp} DEFAULT CURRENT_TIMESTAMP{onUpdate}, "
            + "PRIMARY KEY (table_name, column_name), "
            + $"FOREIGN KEY (key_id) REFERENCES {MetadataSchema}.key_storage(id))");
    }

    /// <summary>
    /// Installs the <c>encrypted_data</c> domain and the functions the trigger needs.
    /// </summary>
    /// <remarks>
    /// The length floor is 45 rather than the 61 this driver always writes, because it counts only the
    /// signature, the type marker and the nonce. Keeping the other driver's number means a value either
    /// driver writes is accepted by either setup.
    /// </remarks>
    private static async Task CreateEncryptedDataTypeAsync(DbConnection plain)
    {
        await ExecuteAsync(plain, "CREATE EXTENSION IF NOT EXISTS pgcrypto");
        await ExecuteAsync(plain, $"DROP DOMAIN IF EXISTS {DomainName} CASCADE");
        await ExecuteAsync(
            plain,
            $"CREATE DOMAIN {DomainName} AS bytea CHECK (length(VALUE) >= 45)");

        await ExecuteAsync(plain, $@"
CREATE OR REPLACE FUNCTION verify_encrypted_data_hmac(data {DomainName}, hmac_key bytea)
RETURNS boolean AS $$
DECLARE
    data_bytes bytea := data::bytea;
BEGIN
    RETURN substring(data_bytes from 1 for 32)
        = hmac(substring(data_bytes from 33), hmac_key, 'sha256');
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;");

        await ExecuteAsync(plain, $@"
CREATE OR REPLACE FUNCTION has_valid_hmac_structure(data {DomainName})
RETURNS boolean AS $$
BEGIN
    RETURN length(data::bytea) >= 45;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;");

        // Reads the HMAC key for the column out of the metadata tables, so it can verify a value without
        // ever holding the data key and therefore without being able to decrypt anything.
        await ExecuteAsync(plain, $@"
CREATE OR REPLACE FUNCTION validate_encrypted_data_hmac()
RETURNS trigger AS $$
DECLARE
    metadata_schema text := '{MetadataSchema}';
    col_name text := TG_ARGV[0];
    col_value {DomainName};
    hmac_key bytea;
    data_bytes bytea;
BEGIN
    EXECUTE format('SELECT ($1).%I', col_name) INTO col_value USING NEW;

    IF col_value IS NOT NULL THEN
        EXECUTE format(
            'SELECT ks.hmac_key FROM %I.encryption_metadata em '
            || 'JOIN %I.key_storage ks ON em.key_id = ks.id '
            || 'WHERE em.table_name = $1 AND em.column_name = $2',
            metadata_schema, metadata_schema
        ) INTO hmac_key USING TG_TABLE_NAME, col_name;

        IF hmac_key IS NULL THEN
            RAISE EXCEPTION 'No HMAC key found for %.%', TG_TABLE_NAME, col_name;
        END IF;

        data_bytes := col_value::bytea;

        IF length(data_bytes) < 45 THEN
            RAISE EXCEPTION 'Invalid encrypted data length for column %', col_name;
        END IF;

        IF substring(data_bytes from 1 for 32)
            != hmac(substring(data_bytes from 33), hmac_key, 'sha256') THEN
            RAISE EXCEPTION 'HMAC verification failed for column %', col_name;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;");
    }

    private static async Task InsertKeyAndMetadataAsync(
        DbConnection plain, string masterKeyArn, string encryptedDataKey, byte[] hmacKey)
    {
        await using (DbCommand insertKey = plain.CreateCommand())
        {
            insertKey.CommandText =
                $"INSERT INTO {MetadataSchema}.key_storage "
                + "(name, master_key_arn, encrypted_data_key, hmac_key, key_spec) "
                + "VALUES (@name, @arn, @dek, @hmac, 'AES_256')";
            Add(insertKey, "@name", KeyName);
            Add(insertKey, "@arn", masterKeyArn);
            Add(insertKey, "@dek", encryptedDataKey);
            Add(insertKey, "@hmac", hmacKey);
            await insertKey.ExecuteNonQueryAsync();
        }

        // The generated identifier is read back by name rather than captured, so the same statement works
        // on both engines without RETURNING or generated-key handling.
        await ExecuteAsync(
            plain,
            $"INSERT INTO {MetadataSchema}.encryption_metadata "
            + "(table_name, column_name, encryption_algorithm, key_id) "
            + $"SELECT '{TableName}', '{EncryptedColumn}', '{Algorithm}', id "
            + $"FROM {MetadataSchema}.key_storage WHERE name = '{KeyName}'");
    }

    /// <summary>
    /// Fails the fixture rather than the first test if the registration did not land, since every test in
    /// the class is meaningless without it.
    /// </summary>
    private static async Task VerifyRegistrationAsync(DbConnection plain)
    {
        await using DbCommand check = plain.CreateCommand();
        check.CommandText =
            $"SELECT COUNT(*) FROM {MetadataSchema}.encryption_metadata "
            + $"WHERE table_name = '{TableName}' AND column_name = '{EncryptedColumn}'";

        object? count = await check.ExecuteScalarAsync();
        if (Convert.ToInt64(count, System.Globalization.CultureInfo.InvariantCulture) == 0)
        {
            throw new InvalidOperationException(
                $"{TableName}.{EncryptedColumn} was not registered for encryption.");
        }
    }

    private static async Task DropAsync(DbConnection plain)
    {
        await ExecuteAsync(plain, $"DROP TABLE IF EXISTS {TableName}");
        await ExecuteAsync(
            plain,
            IsMySql
                ? $"DROP SCHEMA IF EXISTS {MetadataSchema}"
                : $"DROP SCHEMA IF EXISTS {MetadataSchema} CASCADE");

        if (!IsMySql)
        {
            // The domain and the trigger function live outside the metadata schema, so dropping the schema
            // does not take them with it.
            await ExecuteAsync(plain, "DROP FUNCTION IF EXISTS validate_encrypted_data_hmac() CASCADE");
            await ExecuteAsync(plain, $"DROP DOMAIN IF EXISTS {DomainName} CASCADE");
        }
    }

    private static void Add(DbCommand command, string name, object value)
    {
        DbParameter parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
