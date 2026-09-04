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
/// Creates the encryption metadata schema, key registration and test table once for
/// <see cref="KmsEncryptionTests"/>, and removes them again afterwards.
/// </summary>
/// <remarks>
/// <para>
/// A fixture rather than per-test setup because the schema costs a data key generation and several DDL
/// statements, and every test in the class wants the same one.
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
    internal const string MetadataSchema = "kmstest";
    internal const string TableName = "kms_encryption_test";
    internal const string KeyName = "dotnet-integration-key";
    internal const string Algorithm = "AES-256-GCM";

    /// <summary>The leading signature over the rest of a stored value.</summary>
    internal const int SignatureLength = 32;

    /// <summary>The shortest signed region there can be: type marker, nonce and cipher tag.</summary>
    internal const int MinimumSignedLength = 1 + 12 + 16;

    private static readonly DatabaseEngine Engine = TestEnvironment.Env.Info.Request.Engine;

    /// <summary>The environment variable holding the KMS key to encrypt with.</summary>
    /// <remarks>
    /// Named to match what the other drivers' integration tests read, so one variable serves them all.
    /// </remarks>
    internal const string KeyArnVariable = "AWS_KMS_KEY_ARN";

    /// <summary>Why every test in the class skips when no key is configured.</summary>
    internal const string NoKeyReason = KeyArnVariable + " is not set.";

    internal static string? KeyArn => Environment.GetEnvironmentVariable(KeyArnVariable);

    /// <summary>Gets a value indicating whether a key is configured and the schema was created.</summary>
    internal bool Enabled { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the server can check a stored value's signature for itself.
    /// </summary>
    /// <remarks>
    /// Needs pgcrypto, so it is PostgreSQL only, and installing an extension needs a privilege the test
    /// user may not have. Either way the rest of the suite still runs.
    /// </remarks>
    internal bool ServerSideCheckAvailable { get; private set; }

    internal static bool IsMySql => Engine == DatabaseEngine.MYSQL;

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

        await using DbConnection plain = await OpenPlainAsync();
        await DropAsync(plain);
        await CreateAsync(plain, dataKey.KeyId, encryptedDataKey, hmacKey);

        this.Enabled = true;
        this.ServerSideCheckAvailable = !IsMySql && await TryCreateServerSideCheckAsync(plain);
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
            Engine,
            Endpoint(),
            Port(),
            TestEnvironment.Env.Info.DatabaseInfo.Username,
            TestEnvironment.Env.Info.DatabaseInfo.Password,
            TestEnvironment.Env.Info.DatabaseInfo.DefaultDbName,
            enablePooling: false);

        DbConnection connection = IsMySql
            ? new MySqlConnection(connectionString)
            : new NpgsqlConnection(connectionString);

        await connection.OpenAsync();
        return connection;
    }

    // Repeats the endpoint choice IntegrationTestBase makes, because those members are protected and a
    // fixture cannot derive from it.
    private static string Endpoint() =>
        TestEnvironment.Env.Info.Request.Deployment switch
        {
            DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE =>
                TestEnvironment.Env.Info.DatabaseInfo.Instances[0].Host,
            _ => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpoint,
        };

    private static int Port() =>
        TestEnvironment.Env.Info.Request.Deployment switch
        {
            DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE =>
                TestEnvironment.Env.Info.DatabaseInfo.Instances[0].Port,
            _ => TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpointPort,
        };

    private static async Task ExecuteAsync(DbConnection connection, string sql)
    {
        await using DbCommand command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task CreateAsync(
        DbConnection plain, string masterKeyArn, string encryptedDataKey, byte[] hmacKey)
    {
        if (IsMySql)
        {
            await ExecuteAsync(plain, $"CREATE DATABASE {MetadataSchema}");
            await ExecuteAsync(plain, $"CREATE TABLE {MetadataSchema}.key_storage ("
                + "id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, "
                + "master_key_arn VARCHAR(512) NOT NULL, encrypted_data_key TEXT NOT NULL, "
                + "hmac_key VARBINARY(32) NOT NULL, key_spec VARCHAR(50) DEFAULT 'AES_256')");
            await ExecuteAsync(plain, $"CREATE TABLE {MetadataSchema}.encryption_metadata ("
                + "table_name VARCHAR(255) NOT NULL, column_name VARCHAR(255) NOT NULL, "
                + "encryption_algorithm VARCHAR(50) NOT NULL, key_id INT NOT NULL, "
                + "PRIMARY KEY (table_name, column_name))");

            // VARBINARY is the only option: MySQL has no domain type, so the minimum length cannot be
            // enforced in the schema. That is why a readable literal is accepted here and not on PostgreSQL.
            await ExecuteAsync(plain, $"CREATE TABLE {TableName} ("
                + "id INT PRIMARY KEY, secret VARBINARY(512), note VARCHAR(100))");
        }
        else
        {
            await ExecuteAsync(plain, $"CREATE SCHEMA {MetadataSchema}");
            await ExecuteAsync(plain, $"CREATE TABLE {MetadataSchema}.key_storage ("
                + "id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, "
                + "master_key_arn VARCHAR(512) NOT NULL, encrypted_data_key TEXT NOT NULL, "
                + "hmac_key BYTEA NOT NULL, key_spec VARCHAR(50) DEFAULT 'AES_256')");
            await ExecuteAsync(plain, $"CREATE TABLE {MetadataSchema}.encryption_metadata ("
                + "table_name VARCHAR(255) NOT NULL, column_name VARCHAR(255) NOT NULL, "
                + "encryption_algorithm VARCHAR(50) NOT NULL, key_id INTEGER NOT NULL, "
                + "PRIMARY KEY (table_name, column_name))");

            // A domain rather than bare bytea, so the server itself rejects anything too short to be an
            // encrypted value.
            await ExecuteAsync(
                plain,
                $"CREATE DOMAIN {MetadataSchema}.encrypted_data AS bytea "
                + $"CHECK (length(VALUE) >= {SignatureLength + MinimumSignedLength})");
            await ExecuteAsync(plain, $"CREATE TABLE {TableName} ("
                + $"id INTEGER PRIMARY KEY, secret {MetadataSchema}.encrypted_data, note VARCHAR(100))");
        }

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

        await ExecuteAsync(
            plain,
            $"INSERT INTO {MetadataSchema}.encryption_metadata "
            + "(table_name, column_name, encryption_algorithm, key_id) "
            + $"SELECT '{TableName}', 'secret', '{Algorithm}', id "
            + $"FROM {MetadataSchema}.key_storage WHERE name = '{KeyName}'");
    }

    /// <summary>
    /// Installs a function that recomputes a stored value's signature with pgcrypto, so the database can
    /// judge the bytes written by this plugin without using any of its own code.
    /// </summary>
    /// <remarks>
    /// This is the recipe the plugin documentation suggests operators put behind a trigger, so creating it
    /// here also checks that the documented recipe accepts what the plugin writes.
    /// </remarks>
    private static async Task<bool> TryCreateServerSideCheckAsync(DbConnection plain)
    {
        try
        {
            await ExecuteAsync(plain, "CREATE EXTENSION IF NOT EXISTS pgcrypto");
            await ExecuteAsync(
                plain,
                $"CREATE FUNCTION {MetadataSchema}.has_valid_signature(value bytea) RETURNS boolean AS $$"
                + $"  SELECT length(value) >= {SignatureLength + MinimumSignedLength}"
                + "     AND substring(value FOR 32) = hmac("
                + "           substring(value FROM 33),"
                + $"          (SELECT hmac_key FROM {MetadataSchema}.key_storage WHERE name = '{KeyName}'),"
                + "           'sha256')"
                + "$$ LANGUAGE sql");
            return true;
        }
        catch (DbException ex)
        {
            Console.WriteLine(
                $"Server-side signature checking is unavailable, those cases will skip: {ex.Message}");
            return false;
        }
    }

    private static async Task DropAsync(DbConnection plain)
    {
        await ExecuteAsync(plain, $"DROP TABLE IF EXISTS {TableName}");
        await ExecuteAsync(
            plain,
            IsMySql
                ? $"DROP DATABASE IF EXISTS {MetadataSchema}"
                : $"DROP SCHEMA IF EXISTS {MetadataSchema} CASCADE");
    }

    private static void Add(DbCommand command, string name, object value)
    {
        DbParameter parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
