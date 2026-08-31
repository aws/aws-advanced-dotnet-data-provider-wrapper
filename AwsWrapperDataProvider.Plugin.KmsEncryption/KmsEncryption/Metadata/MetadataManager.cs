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

using System.Collections.Concurrent;
using System.Data.Common;
using System.Globalization;
using System.Runtime.CompilerServices;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;
using AwsWrapperDataProvider.Plugin.KmsEncryption.Properties;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Metadata;

/// <summary>
/// Reads which columns are encrypted, and with which key, from the encryption metadata tables.
/// </summary>
/// <remarks>
/// <para>
/// The whole metadata is read in one query and cached, because it is small - one row per encrypted column -
/// and every statement needs it. Concurrent loads share a single query.
/// </para>
/// <para>
/// The query runs on its own connection, opened with this plugin skipped. Using the application's
/// connection would re-enter the plugin while it is deciding what to do with the application's statement,
/// and reading the metadata would itself require the metadata.
/// </para>
/// </remarks>
internal sealed class MetadataManager : IDisposable
{
    private static readonly ILogger<MetadataManager> Logger = LoggerUtils.GetLogger<MetadataManager>();

    /// <summary>
    /// The cached metadata, shared by every connection that reads the same schema as the same user.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Shared rather than per connection because the metadata describes the schema, not the connection: a
    /// per-connection cache re-reads the same handful of rows for every connection an application opens.
    /// </para>
    /// <para>
    /// The key includes the user as well as the server, database, and schema. Without the user, a connection
    /// that is not permitted to read the metadata tables would silently be served another user's copy instead
    /// of failing, which would hand it access it could not obtain for itself.
    /// </para>
    /// </remarks>
    private static readonly MemoryCache Cache = new(new MemoryCacheOptions());

    private static readonly ConcurrentDictionary<string, Lazy<Task<MetadataSnapshot>>> PendingLoads = new();

    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> props;
    private readonly EncryptionConfig config;
    private readonly IConnectionPlugin owner;

    internal MetadataManager(
        IPluginService pluginService,
        Dictionary<string, string> props,
        EncryptionConfig config,
        IConnectionPlugin owner)
    {
        this.pluginService = pluginService;
        this.props = props;
        this.config = config;
        this.owner = owner;
    }

    /// <summary>
    /// Gets a value indicating whether the connected server is MySQL, so that statements are read with the
    /// matching grammar.
    /// </summary>
    /// <remarks>
    /// Every MySQL dialect in this driver derives from <see cref="Driver.Dialects.MySqlDialect"/> and every
    /// PostgreSQL one from <see cref="Driver.Dialects.PgDialect"/>, so the base type is what distinguishes the
    /// two engines. PostgreSQL is assumed when the dialect is not yet known, because its grammar is the
    /// stricter of the two about the quoting that would otherwise be misread.
    /// </remarks>
    internal bool IsMySql => this.pluginService.Dialect is Driver.Dialects.MySqlDialect;

    /// <summary>
    /// Gets the key this connection's metadata is cached under: the server, the database, the user, and the
    /// schema. Anything that could change the answer is part of it.
    /// </summary>
    private string CacheKey
    {
        get
        {
            HostSpec host = this.pluginService.CurrentHostSpec
                ?? throw new EncryptionException(Resources.MetadataManager_CacheKey_NoHostConnected);

            // Taken from the connection rather than the properties, because the database is named by a
            // provider-specific keyword - "Database" for one driver, "Initial Catalog" for another - and is
            // not one of the wrapper's own properties.
            string database = this.pluginService.CurrentConnection?.Database ?? string.Empty;

            return $"{host.Host}:{host.Port}/{database}/"
                + $"{PropertyDefinition.User.GetString(this.props)}/{this.config.MetadataSchema}";
        }
    }

    /// <summary>
    /// Returns the encryption configuration for a column, or <see langword="null"/> when the column is not
    /// encrypted.
    /// </summary>
    internal async Task<ColumnEncryptionConfig?> GetColumnConfigAsync(
        string table,
        string column,
        CancellationToken cancellationToken)
    {
        MetadataSnapshot snapshot = await this.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        return snapshot.Columns.GetValueOrDefault(Key(table, column));
    }

    /// <summary>
    /// Returns the encryption configuration for a column the server identified by table object identifier
    /// and attribute number, or <see langword="null"/> when that column is not encrypted.
    /// </summary>
    internal async Task<ColumnEncryptionConfig?> GetColumnConfigByTableOidAsync(
        uint tableOid,
        int attributeNumber,
        CancellationToken cancellationToken)
    {
        MetadataSnapshot snapshot = await this.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        return snapshot.ByTableOid.GetValueOrDefault(OidKey(tableOid, attributeNumber));
    }

    /// <summary>
    /// Reads the encryption metadata again now, rather than waiting for the cached copy to expire.
    /// </summary>
    /// <remarks>
    /// The new copy is read before the old one is replaced. A refresh that fails therefore leaves the
    /// previous metadata in place and reports the failure, rather than leaving statements with nothing to
    /// work from - an operator refreshing at an unlucky moment must not be able to stop an application that
    /// was working.
    /// </remarks>
    internal async Task RefreshAsync(CancellationToken cancellationToken = default)
    {
        MetadataSnapshot snapshot = await this.LoadAsync(cancellationToken).ConfigureAwait(false);

        // With caching switched off every statement reads the metadata anyway, so there is nothing to
        // replace; the read above still confirms it is readable.
        if (this.config.MetadataCacheEnabled)
        {
            Cache.Set(this.CacheKey, snapshot, this.config.MetadataCacheExpiration);
            Logger.LogDebug(Resources.MetadataManager_RefreshAsync_Refreshed, this.CacheKey);
        }
    }

    /// <summary>Returns whether any column of <paramref name="table"/> is encrypted.</summary>
    internal async Task<bool> HasEncryptedColumnsAsync(string table, CancellationToken cancellationToken)
    {
        MetadataSnapshot snapshot = await this.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        return snapshot.Tables.Contains(table);
    }

    /// <summary>
    /// Nothing to release. The cache is shared, so disposing one manager must not empty it for the others.
    /// </summary>
    public void Dispose()
    {
    }

    /// <summary>
    /// Discards all cached encryption metadata, so the next statement reads it again.
    /// </summary>
    /// <remarks>
    /// Registering a column is something an operator does while applications are running. Without this the
    /// change is only noticed when the cached copy expires, up to
    /// <see cref="PropertyDefinition.KmsMetadataCacheExpirationMinutes"/> later.
    /// </remarks>
    internal static void ClearCache()
    {
        Cache.Clear();
        PendingLoads.Clear();
        Logger.LogDebug(Resources.MetadataManager_ClearCache_Cleared);
    }

    /// <summary>
    /// Validates that a configured schema name is a plain identifier.
    /// </summary>
    /// <remarks>
    /// The schema name comes from a connection property and is placed directly into the metadata query,
    /// which cannot be parameterised because it names an object rather than supplies a value. Restricting it
    /// to letters, digits, and underscores is what makes that safe.
    /// </remarks>
    internal static string ValidateSchemaName(string schema)
    {
        if (schema.Length == 0 || schema.Length > 63
            || !schema.All(c => c == '_' || char.IsLetterOrDigit(c))
            || char.IsDigit(schema[0]))
        {
            throw new ArgumentException(string.Format(
                CultureInfo.CurrentCulture,
                Resources.MetadataManager_ValidateSchemaName_NotAnIdentifier,
                PropertyDefinition.KmsEncryptionMetadataSchema.Name,
                schema));
        }

        return schema;
    }

    private static string Key(string table, string column) =>
        $"{table.ToLowerInvariant()}.{column.ToLowerInvariant()}";

    private static string OidKey(uint tableOid, int attributeNumber) =>
        $"{tableOid}/{attributeNumber}";

    /// <summary>
    /// Maps each encrypted column onto the numbers the server uses to identify it in a result set.
    /// </summary>
    /// <remarks>
    /// PostgreSQL reports a column's owning table and position numerically in every result set, but does not
    /// report their names, so a decrypting reader cannot recognise a column by name alone. Translating the
    /// names once here, on the metadata connection, avoids needing a catalogue query while a reader is open.
    /// MySQL has no such catalogue, so there the query is expected to fail and an empty map is the right
    /// answer - identification falls back to the names the driver reports. On PostgreSQL a failure is not
    /// expected and must not be swallowed: without this map nothing is recognised as encrypted, so every
    /// value would be handed to the application as raw bytes with no indication that anything went wrong.
    /// </remarks>
    private static async Task<IReadOnlyDictionary<string, ColumnEncryptionConfig>> ResolveTableOidsAsync(
        DbConnection connection,
        IReadOnlyDictionary<string, ColumnEncryptionConfig> columns,
        bool mySql,
        CancellationToken cancellationToken)
    {
        var byOid = new Dictionary<string, ColumnEncryptionConfig>(StringComparer.Ordinal);
        if (columns.Count == 0)
        {
            return byOid;
        }

        try
        {
            DbCommand command = connection.CreateCommand();
            await using ConfiguredAsyncDisposable commandScope = command.ConfigureAwait(false);
            command.CommandText =
                "SELECT c.oid, a.attnum, c.relname, a.attname "
                + "FROM pg_catalog.pg_class c "
                + "JOIN pg_catalog.pg_attribute a ON a.attrelid OPERATOR(pg_catalog.=) c.oid "
                + "WHERE a.attnum OPERATOR(pg_catalog.>) 0 AND NOT a.attisdropped";

            await using DbDataReader reader = await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);

            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                uint oid = Convert.ToUInt32(reader.GetValue(0), System.Globalization.CultureInfo.InvariantCulture);
                int attnum = Convert.ToInt32(reader.GetValue(1), System.Globalization.CultureInfo.InvariantCulture);
                string table = reader.GetString(2);
                string column = reader.GetString(3);

                if (columns.TryGetValue(Key(table, column), out ColumnEncryptionConfig? config))
                {
                    byOid[OidKey(oid, attnum)] = config;
                }
            }
        }
        catch (Exception ex) when (mySql)
        {
            Logger.LogDebug(ex, Resources.MetadataManager_ResolveTableOidsAsync_NoCatalogue);
            return byOid;
        }
        catch (Exception ex)
        {
            // Deliberately not swallowed. An empty map means no column is recognised as encrypted, and the
            // application would silently receive ciphertext, so the metadata load fails instead.
            Logger.LogWarning(ex, Resources.MetadataManager_ResolveTableOidsAsync_MappingFailed);
            throw;
        }

        return byOid;
    }

    private Task<MetadataSnapshot> GetSnapshotAsync(CancellationToken cancellationToken)
    {
        if (this.config.MetadataCacheEnabled
            && Cache.TryGetValue(this.CacheKey, out MetadataSnapshot? cached)
            && cached is not null)
        {
            return Task.FromResult(cached);
        }

        // Concurrent misses share one query. Without this, a burst of statements on a cold cache would each
        // open a connection and read the metadata.
        string key = this.CacheKey;
        Lazy<Task<MetadataSnapshot>> pending = PendingLoads.GetOrAdd(
            key,
            _ => new Lazy<Task<MetadataSnapshot>>(
                () => this.LoadAndCacheAsync(cancellationToken),
                LazyThreadSafetyMode.ExecutionAndPublication));

        return AwaitSharedLoadAsync(pending, key);
    }

    /// <summary>
    /// Awaits a load that is being shared with any other caller that arrived while it was running, and
    /// removes it from <see cref="PendingLoads"/> once it has finished.
    /// </summary>
    /// <remarks>
    /// The removal is what stops the shared entry outliving its usefulness, and both outcomes need it:
    /// <list type="bullet">
    /// <item><description>
    /// A load that failed would otherwise have its exception replayed to every later caller, turning a
    /// momentary failure into a permanent one.
    /// </description></item>
    /// <item><description>
    /// A load that succeeded would otherwise be found by the next caller after the cached copy expires, and
    /// the same stale snapshot would be returned for the life of the process - the expiry silently defeated.
    /// </description></item>
    /// </list>
    /// </remarks>
    private static async Task<MetadataSnapshot> AwaitSharedLoadAsync(
        Lazy<Task<MetadataSnapshot>> pending,
        string key)
    {
        try
        {
            return await pending.Value.ConfigureAwait(false);
        }
        finally
        {
            // Removed only if it is still this load, so that a later caller's fresh attempt is not
            // discarded along with this one.
            PendingLoads.TryRemove(new KeyValuePair<string, Lazy<Task<MetadataSnapshot>>>(key, pending));
        }
    }

    private async Task<MetadataSnapshot> LoadAndCacheAsync(CancellationToken cancellationToken)
    {
        MetadataSnapshot snapshot = await this.LoadAsync(cancellationToken).ConfigureAwait(false);

        if (this.config.MetadataCacheEnabled)
        {
            Cache.Set(this.CacheKey, snapshot, this.config.MetadataCacheExpiration);
        }

        return snapshot;
    }

    private async Task<MetadataSnapshot> LoadAsync(CancellationToken cancellationToken)
    {
        string schema = ValidateSchemaName(this.config.MetadataSchema);
        HostSpec hostSpec = this.pluginService.CurrentHostSpec
            ?? throw new EncryptionException(Resources.MetadataManager_CacheKey_NoHostConnected);

        DbConnection connection = await this.pluginService
            .OpenConnection(hostSpec, this.props, this.owner, async: true)
            .ConfigureAwait(false);

        try
        {
            await using (connection.ConfigureAwait(false))
            {
                if (connection.State != System.Data.ConnectionState.Open)
                {
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                }

                return await ReadAsync(connection, schema, this.IsMySql, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (EncryptionException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new EncryptionException(
                string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.MetadataManager_LoadAsync_MetadataUnreadable,
                    schema),
                ex);
        }
    }

    private static async Task<MetadataSnapshot> ReadAsync(
        DbConnection connection,
        string schema,
        bool mySql,
        CancellationToken cancellationToken)
    {
        DbCommand command = connection.CreateCommand();
        await using ConfiguredAsyncDisposable commandScope = command.ConfigureAwait(false);

        // The schema is validated above; every other part of this statement is fixed text.
        //
        // A LEFT JOIN is deliberate. With an inner join, a metadata row whose key is missing from
        // key_storage would simply not be returned, the column would look unencrypted, and the value would
        // be written in plain text. Reading the row and failing when the key material is absent is the only
        // safe behaviour: a broken key reference must stop the write, not silently disable encryption.
        // key_storage is identified by its integer primary key, which is what encryption_metadata.key_id
        // references. ks.name is carried alongside it so that a failure can name the key the way an operator
        // set it up rather than by number alone.
        command.CommandText =
            "SELECT em.table_name, em.column_name, em.encryption_algorithm, em.key_id, "
            + "ks.id, ks.name, ks.master_key_arn, ks.encrypted_data_key, ks.hmac_key, ks.key_spec "
            + $"FROM {schema}.encryption_metadata em "
            + $"LEFT JOIN {schema}.key_storage ks ON ks.id = em.key_id";

        var columns = new Dictionary<string, ColumnEncryptionConfig>(StringComparer.Ordinal);
        var tables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Scoped so the reader is closed before the catalogue query below. A connection can only have one
        // reader open at a time, and leaving this one open makes that second query fail.
        await using (DbDataReader reader = await command
            .ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                string table = reader.GetString(0);
                string column = reader.GetString(1);
                string algorithm = reader.GetString(2);
                string referencedKey = reader.GetValue(3).ToString() ?? "<null>";

                // Null here means the metadata references a key that is not in key_storage. The column is still
                // registered, without key material, so any attempt to write it fails with a message naming the
                // missing key rather than quietly storing readable data.
                DataKeyMetadata? keyMetadata = await reader.IsDBNullAsync(4, cancellationToken).ConfigureAwait(false)
                    ? null
                    : new DataKeyMetadata(
                        keyId: $"{reader.GetValue(5)} (id {reader.GetValue(4)})",
                        masterKeyArn: reader.GetString(6),
                        encryptedDataKey: reader.GetString(7),
                        hmacKey: (byte[])reader.GetValue(8),
                        keySpec: reader.GetString(9));

                columns[Key(table, column)] = new ColumnEncryptionConfig(
                    table,
                    column,
                    keyMetadata?.KeyId ?? referencedKey,
                    algorithm,
                    keyMetadata);
                tables.Add(table);
            }
        }

        Logger.LogDebug(
            Resources.MetadataManager_ReadAsync_Loaded,
            columns.Count,
            tables.Count,
            schema);

        IReadOnlyDictionary<string, ColumnEncryptionConfig> byOid =
            await ResolveTableOidsAsync(connection, columns, mySql, cancellationToken).ConfigureAwait(false);

        return new MetadataSnapshot(columns, tables, byOid);
    }

    /// <summary>The metadata as read at one point in time.</summary>
    private sealed class MetadataSnapshot
    {
        internal MetadataSnapshot(
            IReadOnlyDictionary<string, ColumnEncryptionConfig> columns,
            IReadOnlySet<string> tables,
            IReadOnlyDictionary<string, ColumnEncryptionConfig> byTableOid)
        {
            this.Columns = columns;
            this.Tables = tables;
            this.ByTableOid = byTableOid;
        }

        internal IReadOnlyDictionary<string, ColumnEncryptionConfig> Columns { get; }

        internal IReadOnlySet<string> Tables { get; }

        /// <summary>
        /// Gets the same columns keyed by "tableOid/attributeNumber", for identifying a column in a result
        /// set where the server reports numbers rather than names.
        /// </summary>
        internal IReadOnlyDictionary<string, ColumnEncryptionConfig> ByTableOid { get; }
    }
}
