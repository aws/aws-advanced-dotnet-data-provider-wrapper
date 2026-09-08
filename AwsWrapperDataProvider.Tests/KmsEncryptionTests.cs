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
using System.Text;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Metadata;
using AwsWrapperDataProvider.Tests.Container.Utils;

namespace AwsWrapperDataProvider.Tests;

/// <summary>
/// End-to-end checks for the kmsEncryption plugin against a real cluster and a real AWS KMS key.
/// </summary>
/// <remarks>
/// <para>
/// Requires a KMS key, supplied through the <c>AWS_KMS_KEY_ARN</c> environment variable. Every test skips
/// when it is absent, so an environment without one still passes.
/// </para>
/// <para>
/// <see cref="KmsEncryptionTestFixture"/> creates the metadata schema, key registration and table once for
/// the class and drops them again afterwards, so the suite leaves the database as it found it.
/// </para>
/// <para>
/// A second, plugin-free connection is used wherever the stored bytes matter. Reading back through the plugin
/// decrypts, and a round trip that starts and ends in plaintext cannot tell real encryption from doing
/// nothing at all.
/// </para>
/// </remarks>
public class KmsEncryptionTests : IntegrationTestBase,
    IClassFixture<KmsEncryptionTestFixture>
{
    private const string MetadataSchema = KmsEncryptionTestFixture.MetadataSchema;
    private const string TableName = KmsEncryptionTestFixture.TableName;
    private const string Column = KmsEncryptionTestFixture.EncryptedColumn;

    private const string Ssn1 = "123-45-6789";
    private const string Ssn2 = "987-65-4321";

    /// <summary>The fixed overhead of a stored value: signature, type marker, nonce and cipher tag.</summary>
    private const int StoredOverhead =
        KmsEncryptionTestFixture.SignatureLength + KmsEncryptionTestFixture.MinimumSignedLength;

    private readonly KmsEncryptionTestFixture fixture;

    public KmsEncryptionTests(KmsEncryptionTestFixture fixture)
    {
        this.fixture = fixture;
    }

    private static bool IsMySql => Engine == DatabaseEngine.MYSQL;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();

        ConnectionPluginChainBuilder.RegisterPluginFactory<KmsEncryptionPluginFactory>(
            PluginCodes.KmsEncryption);

        // The metadata cache is shared across connections and outlives a single test, so it is cleared
        // between tests to keep them independent.
        KmsEncryptionPlugin.ClearCache();
    }

    /// <summary>
    /// The registration the plugin reads is present and names the expected algorithm and key.
    /// </summary>
    /// <remarks>
    /// This checks the fixture rather than the plugin - it reads the metadata tables directly, on a
    /// plugin-free connection. It is here because every other test in the class is meaningless if the
    /// registration is missing or wrong, and a failure here says so plainly instead of surfacing as an
    /// unencrypted value several tests later.
    /// </remarks>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Database", "mysql-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestEncryptionMetadataSetup()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);

        await using DbConnection plain = await KmsEncryptionTestFixture.OpenPlainAsync();
        await using DbCommand command = plain.CreateCommand();
        command.CommandText =
            $"SELECT m.encryption_algorithm, k.name FROM {MetadataSchema}.encryption_metadata m "
            + $"JOIN {MetadataSchema}.key_storage k ON k.id = m.key_id "
            + "WHERE m.table_name = @table AND m.column_name = '" + Column + "'";
        AddParameter(command, "@table", TableName);

        await using DbDataReader reader = await command.ExecuteReaderAsync(
            TestContext.Current.CancellationToken);

        Assert.True(
            await reader.ReadAsync(TestContext.Current.CancellationToken),
            $"{TableName}.{Column} is not registered for encryption.");
        Assert.Equal(KmsEncryptionTestFixture.Algorithm, reader.GetString(0));
        Assert.Equal(KmsEncryptionTestFixture.KeyName, reader.GetString(1));
    }

    /// <summary>
    /// The database validates a stored value's signature for itself, with pgcrypto and no help from this
    /// driver. This is the strongest available check that the stored layout is right, because nothing about
    /// it comes from the code under test.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestStoredValueIsVerifiedByTheServer()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);
        Assert.SkipWhen(IsMySql, "the trigger and pgcrypto are PostgreSQL only.");

        int id = NewId();
        await using (AwsWrapperConnection connection = await this.OpenEncryptedAsync())
        {
            await InsertAsync(connection, id, Ssn1);
        }

        await using DbConnection plain = await KmsEncryptionTestFixture.OpenPlainAsync();
        await using DbCommand command = plain.CreateCommand();
        command.CommandText =
            $"SELECT verify_encrypted_data_hmac({Column}, k.hmac_key) "
            + $"FROM {TableName} t, {MetadataSchema}.key_storage k "
            + $"WHERE t.id = @id AND k.name = '{KmsEncryptionTestFixture.KeyName}'";
        AddParameter(command, "@id", id);

        object? verified = await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        Assert.True(
            Assert.IsType<bool>(verified), "the server did not accept the stored value's signature.");
    }

    /// <summary>
    /// The trigger rejects bytes that are long enough to look encrypted but are not.
    /// </summary>
    /// <remarks>
    /// This is the case the plugin cannot see at all: a migration script, an administrative tool, or an
    /// application connecting without the plugin. The value is written on a plugin-free connection and is
    /// long enough to satisfy the column's length floor, so the signature check is the only thing that can
    /// reject it - which is what distinguishes the trigger from the domain.
    /// </remarks>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestTriggerRejectsBytesThatAreNotEncrypted()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);
        Assert.SkipWhen(IsMySql, "the trigger and pgcrypto are PostgreSQL only.");

        byte[] notEncrypted = RandomNumberGenerator.GetBytes(StoredOverhead);

        await using DbConnection plain = await KmsEncryptionTestFixture.OpenPlainAsync();
        await using DbCommand insert = plain.CreateCommand();
        insert.CommandText = $"INSERT INTO {TableName} (id, {Column}) VALUES (@id, @value)";
        AddParameter(insert, "@id", NewId());
        AddParameter(insert, "@value", notEncrypted);

        DbException thrown = await Assert.ThrowsAnyAsync<DbException>(
            () => insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        Assert.Contains("HMAC", thrown.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The trigger rejects a stored value whose bytes have been altered.
    /// </summary>
    /// <remarks>
    /// The value starts out genuinely encrypted, so it has a valid signature and the right length; only one
    /// byte of it is changed. Nothing about its shape gives it away, which is what makes this the case the
    /// signature exists for - and it is caught as the row goes in rather than whenever it is next read.
    /// </remarks>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestTriggerRejectsATamperedValue()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);
        Assert.SkipWhen(IsMySql, "the trigger and pgcrypto are PostgreSQL only.");

        int id = NewId();
        await using (AwsWrapperConnection connection = await this.OpenEncryptedAsync())
        {
            await InsertAsync(connection, id, Ssn1);
        }

        byte[] tampered = await this.ReadStoredBytesAsync(id);

        // The last byte is inside the signed region, so the signature no longer covers what is there.
        tampered[^1] ^= 0xFF;

        await using DbConnection plain = await KmsEncryptionTestFixture.OpenPlainAsync();
        await using DbCommand update = plain.CreateCommand();
        update.CommandText = $"UPDATE {TableName} SET {Column} = @value WHERE id = @id";
        AddParameter(update, "@value", tampered);
        AddParameter(update, "@id", id);

        DbException thrown = await Assert.ThrowsAnyAsync<DbException>(
            () => update.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        Assert.Contains("HMAC", thrown.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>A value written through the plugin is stored encrypted and reads back intact.</summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Database", "mysql-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestBasicEncryption()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);
        int id = NewId();

        await using (AwsWrapperConnection connection = await this.OpenEncryptedAsync())
        {
            await InsertAsync(connection, id, Ssn1);
            Assert.Equal(Ssn1, await ReadThroughPluginAsync(connection, id));
        }

        byte[] stored = await this.ReadStoredBytesAsync(id);
        Assert.Equal(StoredOverhead + Ssn1.Length, stored.Length);
        Assert.DoesNotContain(
            Convert.ToHexString(Encoding.UTF8.GetBytes(Ssn1)), Convert.ToHexString(stored));
    }

    /// <summary>An UPDATE encrypts too, not only an INSERT.</summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Database", "mysql-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestUpdateEncryption()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);
        int id = NewId();

        await using AwsWrapperConnection connection = await this.OpenEncryptedAsync();
        await InsertAsync(connection, id, Ssn1);

        await using (DbCommand update = connection.CreateCommand())
        {
            update.CommandText = $"UPDATE {TableName} SET {Column} = @secret WHERE id = @id";
            AddParameter(update, "@secret", Ssn2);
            AddParameter(update, "@id", id);
            Assert.Equal(1, await update.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        }

        Assert.Equal(Ssn2, await ReadThroughPluginAsync(connection, id));

        byte[] stored = await this.ReadStoredBytesAsync(id);
        Assert.Equal(StoredOverhead + Ssn2.Length, stored.Length);
    }

    /// <summary>
    /// Both branches of an upsert encrypt. The conflict branch is a second write to the encrypted column,
    /// and a plugin that maps only the VALUES parameter looks correct after the first write.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Database", "mysql-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestUpsertEncryptsBothBranches()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);
        int id = NewId();

        string upsert = IsMySql
            ? $"INSERT INTO {TableName} (id, {Column}) VALUES (@id, @secret) "
                + $"ON DUPLICATE KEY UPDATE {Column} = @secret"
            : $"INSERT INTO {TableName} (id, {Column}) VALUES (@id, @secret) "
                + $"ON CONFLICT (id) DO UPDATE SET {Column} = @secret";

        await using AwsWrapperConnection connection = await this.OpenEncryptedAsync();

        var storedForms = new List<string>();
        foreach (string value in new[] { Ssn1, Ssn2 })
        {
            await using DbCommand command = connection.CreateCommand();
            command.CommandText = upsert;
            AddParameter(command, "@id", id);
            AddParameter(command, "@secret", value);
            await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

            byte[] stored = await this.ReadStoredBytesAsync(id);
            Assert.Equal(StoredOverhead + value.Length, stored.Length);
            Assert.DoesNotContain(
                Convert.ToHexString(Encoding.UTF8.GetBytes(value)), Convert.ToHexString(stored));
            storedForms.Add(Convert.ToHexString(stored));
        }

        // The second write must genuinely have replaced the first.
        Assert.NotEqual(storedForms[0], storedForms[1]);
        Assert.Equal(Ssn2, await ReadThroughPluginAsync(connection, id));
    }

    /// <summary>
    /// Every command of a batch is encrypted against its own columns. Values of different lengths are used
    /// so that a value matched to the wrong command's column would change the stored length.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Database", "mysql-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestBatchEncryptsEveryCommand()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);
        (int Id, string Secret)[] rows =
        {
            (NewId(), "111-11-1111"),
            (NewId(), "22-222"),
            (NewId(), "333-33-3333-333"),
        };

        await using AwsWrapperConnection connection = await this.OpenEncryptedAsync();

        await using (DbBatch batch = connection.CreateBatch())
        {
            foreach ((int id, string secret) in rows)
            {
                DbBatchCommand command = batch.CreateBatchCommand();
                command.CommandText = $"INSERT INTO {TableName} (id, {Column}) VALUES (@id, @secret)";
                AddBatchParameter(command, "@id", id);
                AddBatchParameter(command, "@secret", secret);
                batch.BatchCommands.Add(command);
            }

            await batch.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        foreach ((int id, string secret) in rows)
        {
            byte[] stored = await this.ReadStoredBytesAsync(id);
            Assert.Equal(StoredOverhead + Encoding.UTF8.GetByteCount(secret), stored.Length);
        }
    }

    /// <summary>
    /// A batch of SELECTs decrypts across the result-set boundary, which is the only path that makes the
    /// decrypting reader rebind its columns and keys part way through.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Database", "mysql-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestBatchDecryptsAcrossResultSets()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);
        (int Id, string Secret)[] rows = { (NewId(), Ssn1), (NewId(), Ssn2) };

        await using AwsWrapperConnection connection = await this.OpenEncryptedAsync();
        foreach ((int id, string secret) in rows)
        {
            await InsertAsync(connection, id, secret);
        }

        await using DbBatch batch = connection.CreateBatch();
        foreach ((int id, string _) in rows)
        {
            DbBatchCommand command = batch.CreateBatchCommand();
            command.CommandText = $"SELECT {Column} FROM {TableName} WHERE id = @id";
            AddBatchParameter(command, "@id", id);
            batch.BatchCommands.Add(command);
        }

        await using DbDataReader reader = await batch.ExecuteReaderAsync(
            TestContext.Current.CancellationToken);

        var read = new List<string>();
        do
        {
            while (await reader.ReadAsync(TestContext.Current.CancellationToken))
            {
                read.Add(reader.GetString(0));
            }
        }
        while (await reader.NextResultAsync(TestContext.Current.CancellationToken));

        Assert.Equal(rows.Select(r => r.Secret), read);
    }

    /// <summary>
    /// A value bound as a parameter to a column that is not registered must be stored as it was supplied.
    /// Encrypting an unregistered column would corrupt data silently.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Database", "mysql-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestUnregisteredColumnIsLeftAlone()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);
        int id = NewId();
        const string plainValue = "not a secret";

        await using AwsWrapperConnection connection = await this.OpenEncryptedAsync();

        await using (DbCommand insert = connection.CreateCommand())
        {
            insert.CommandText =
                $"INSERT INTO {TableName} (id, {Column}, email) VALUES (@id, @secret, @email)";
            AddParameter(insert, "@id", id);
            AddParameter(insert, "@secret", Ssn1);
            AddParameter(insert, "@email", plainValue);
            await insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        await using DbConnection plain = await KmsEncryptionTestFixture.OpenPlainAsync();
        await using DbCommand check = plain.CreateCommand();
        check.CommandText = $"SELECT email FROM {TableName} WHERE id = @id";
        AddParameter(check, "@id", id);

        Assert.Equal(plainValue, await check.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// A NULL stays NULL. Encrypting it would store ciphertext of nothing, which no longer reads back as
    /// NULL and defeats IS NULL predicates.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Database", "mysql-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestNullStaysNull()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);
        int id = NewId();

        await using AwsWrapperConnection connection = await this.OpenEncryptedAsync();

        await using (DbCommand insert = connection.CreateCommand())
        {
            insert.CommandText = $"INSERT INTO {TableName} (id, {Column}) VALUES (@id, @secret)";
            AddParameter(insert, "@id", id);
            AddParameter(insert, "@secret", DBNull.Value);
            await insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        await using DbCommand select = connection.CreateCommand();
        select.CommandText = $"SELECT {Column} FROM {TableName} WHERE id = @id";
        AddParameter(select, "@id", id);

        await using DbDataReader reader = await select.ExecuteReaderAsync(
            TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
        Assert.True(await reader.IsDBNullAsync(0, TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// Values of every supported type round-trip, which exercises each type marker and its byte layout
    /// against a real column rather than only in memory.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Database", "mysql-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestSupportedTypesRoundTrip()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);

        object[] values =
        {
            "text value",
            42,
            9_000_000_000L,
            3.5d,
            1.5f,
            true,
            123.456m,
            new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Unspecified),
            new DateOnly(2026, 3, 4),
            new TimeOnly(10, 15, 30),
            new DateTimeOffset(2026, 3, 4, 5, 6, 7, TimeSpan.Zero),
            new byte[] { 1, 2, 3, 4 },
        };

        await using AwsWrapperConnection connection = await this.OpenEncryptedAsync();

        foreach (object value in values)
        {
            int id = NewId();
            await using (DbCommand insert = connection.CreateCommand())
            {
                insert.CommandText = $"INSERT INTO {TableName} (id, {Column}) VALUES (@id, @secret)";
                AddParameter(insert, "@id", id);
                AddParameter(insert, "@secret", value);
                await insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }

            await using DbCommand select = connection.CreateCommand();
            select.CommandText = $"SELECT {Column} FROM {TableName} WHERE id = @id";
            AddParameter(select, "@id", id);

            await using DbDataReader reader = await select.ExecuteReaderAsync(
                TestContext.Current.CancellationToken);
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));

            object read = reader.GetValue(0);
            Assert.Equal(value, read);
        }
    }

    /// <summary>
    /// The metadata is read once for a burst of statements rather than once per statement, and clearing the
    /// cache makes the next statement read it again - which is what an operator relies on after registering
    /// a column.
    /// </summary>
    /// <remarks>
    /// Both halves are measured with <see cref="MetadataManager.LoadCount"/>, because the number of reads is
    /// the only thing that distinguishes a working cache from a disabled one: every statement succeeds either
    /// way, so asserting only on the values would pass with caching switched off entirely.
    /// </remarks>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Database", "mysql-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestMetadataIsCachedAndCanBeCleared()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);

        // InitializeAsync cleared the cache, so the first statement below is a certain miss. The count is
        // captured rather than assumed to be zero, since it covers the whole process.
        int loadsAtStart = MetadataManager.LoadCount;

        await using AwsWrapperConnection connection = await this.OpenEncryptedAsync();

        for (int i = 0; i < 5; i++)
        {
            await InsertAsync(connection, NewId(), Ssn1);
        }

        Assert.Equal(loadsAtStart + 1, MetadataManager.LoadCount);

        KmsEncryptionPlugin.ClearCache();

        int id = NewId();
        await InsertAsync(connection, id, Ssn2);

        // Exactly one more: the clear forced a fresh read rather than the statement failing or carrying on
        // with the copy that was just discarded.
        Assert.Equal(loadsAtStart + 2, MetadataManager.LoadCount);
        Assert.Equal(Ssn2, await ReadThroughPluginAsync(connection, id));
    }

    /// <summary>
    /// A literal cannot be intercepted, so it reaches the column as written. On PostgreSQL the column
    /// constraint rejects it; MySQL has no equivalent, so the value is stored readable and the test asserts
    /// that, because it is the behaviour the documentation warns about.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-kms")]
    [Trait("Database", "mysql-kms")]
    [Trait("Engine", "aurora")]
    public async Task TestLiteralIsNotEncrypted()
    {
        Assert.SkipUnless(this.fixture.Enabled, KmsEncryptionTestFixture.NoKeyReason);
        int id = NewId();

        await using AwsWrapperConnection connection = await this.OpenEncryptedAsync();
        await using DbCommand insert = connection.CreateCommand();
        insert.CommandText = $"INSERT INTO {TableName} (id, {Column}) VALUES ({id}, '{Ssn1}')";

        if (IsMySql)
        {
            // No column constraint exists on MySQL, so the readable value is accepted.
            await insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            byte[] stored = await this.ReadStoredBytesAsync(id);
            Assert.Equal(Ssn1, Encoding.UTF8.GetString(stored));
        }
        else
        {
            // The domain's length check rejects it.
            await Assert.ThrowsAnyAsync<DbException>(
                () => insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        }
    }

    private static int NewId() => Random.Shared.Next(1, int.MaxValue);

    private static void AddParameter(DbCommand command, string name, object value)
    {
        DbParameter parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    private static void AddBatchParameter(DbBatchCommand command, string name, object value)
    {
        DbParameter parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    private static async Task InsertAsync(DbConnection connection, int id, object secret)
    {
        await using DbCommand insert = connection.CreateCommand();
        insert.CommandText = $"INSERT INTO {TableName} (id, {Column}) VALUES (@id, @secret)";
        AddParameter(insert, "@id", id);
        AddParameter(insert, "@secret", secret);
        Assert.Equal(1, await insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
    }

    private static async Task<string> ReadThroughPluginAsync(DbConnection connection, int id)
    {
        await using DbCommand select = connection.CreateCommand();
        select.CommandText = $"SELECT {Column} FROM {TableName} WHERE id = @id";
        AddParameter(select, "@id", id);

        await using DbDataReader reader = await select.ExecuteReaderAsync(
            TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
        return reader.GetString(0);
    }

    private string PluginConnectionString() =>
        ConnectionStringHelper.GetUrl(
            Engine, Endpoint, Port, Username, Password, DefaultDbName, enablePooling: false)
        + $";Plugins={PluginCodes.KmsEncryption}"
        + $";KmsRegion={TestEnvironment.Env.Info.Region}"
        + $";KmsEncryptionMetadataSchema={MetadataSchema}";

    private async Task<AwsWrapperConnection> OpenEncryptedAsync()
    {
        AwsWrapperConnection connection =
            AuroraUtils.CreateAwsWrapperConnection(Engine, this.PluginConnectionString());
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return connection;
    }

    private async Task<byte[]> ReadStoredBytesAsync(int id)
    {
        await using DbConnection plain = await KmsEncryptionTestFixture.OpenPlainAsync();
        await using DbCommand command = plain.CreateCommand();
        command.CommandText = $"SELECT {Column} FROM {TableName} WHERE id = @id";
        AddParameter(command, "@id", id);

        object? stored = await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        return Assert.IsType<byte[]>(stored);
    }
}
