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

using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Security.Cryptography;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Metadata;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;
using AwsWrapperDataProvider.Plugin.KmsEncryption.Properties;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Wrapper;

/// <summary>
/// Wraps a reader and decrypts the columns that the encryption metadata says are encrypted.
/// </summary>
/// <remarks>
/// <para>
/// <b>Only the abstract members of <see cref="DbDataReader"/> are overridden.</b> That is deliberate rather
/// than an omission. The base class implements the rest in terms of the abstract ones -
/// <c>GetFieldValue&lt;T&gt;</c> calls <see cref="GetValue"/>, <c>GetTextReader</c> calls
/// <see cref="GetString"/>, <c>GetStream</c> calls <see cref="GetBytes"/>, <c>IsDBNullAsync</c> calls
/// <see cref="IsDBNull"/> - so implementing the abstract set covers every accessor. Adding a delegating
/// override for any of the inherited members would bypass that and return ciphertext.
/// </para>
/// <para>
/// The data key for each encrypted column is resolved once, when the reader is created, and held for its
/// lifetime. Accessors are therefore pure decryption with no input or output of their own, which matters
/// because they are synchronous and must not block on a network call. The keys are private copies, scrubbed
/// when the reader is disposed.
/// </para>
/// </remarks>
internal sealed class DecryptingDataReader : DbDataReader, IWrapper
{
    private readonly DbDataReader inner;
    private readonly EncryptionService encryptionService;
    private readonly MetadataManager metadataManager;
    private readonly Func<ColumnEncryptionConfig, CancellationToken, Task<byte[]>> dataKeyResolver;
    private Dictionary<int, EncryptedColumn> encryptedByOrdinal;
    private bool disposed;

    private DecryptingDataReader(
        DbDataReader inner,
        EncryptionService encryptionService,
        MetadataManager metadataManager,
        Func<ColumnEncryptionConfig, CancellationToken, Task<byte[]>> dataKeyResolver,
        Dictionary<int, EncryptedColumn> encryptedByOrdinal)
    {
        this.inner = inner;
        this.encryptionService = encryptionService;
        this.metadataManager = metadataManager;
        this.dataKeyResolver = dataKeyResolver;
        this.encryptedByOrdinal = encryptedByOrdinal;
    }

    public override int Depth => this.inner.Depth;

    public override int FieldCount => this.inner.FieldCount;

    public override bool HasRows => this.inner.HasRows;

    public override bool IsClosed => this.inner.IsClosed;

    public override int RecordsAffected => this.inner.RecordsAffected;

    public override object this[int ordinal] => this.GetValue(ordinal);

    public override object this[string name] => this.GetValue(this.GetOrdinal(name));

    /// <summary>
    /// Wraps <paramref name="inner"/> when any of its columns are encrypted, and returns it unchanged
    /// otherwise so that ordinary queries pay nothing.
    /// </summary>
    internal static async Task<DbDataReader> CreateAsync(
        DbDataReader inner,
        MetadataManager metadataManager,
        EncryptionService encryptionService,
        Func<ColumnEncryptionConfig, CancellationToken, Task<byte[]>> dataKeyResolver,
        CancellationToken cancellationToken)
    {
        Dictionary<int, EncryptedColumn> map = await ResolveAsync(
            inner, metadataManager, dataKeyResolver, cancellationToken).ConfigureAwait(false);

        return map.Count == 0
            ? inner
            : new DecryptingDataReader(inner, encryptionService, metadataManager, dataKeyResolver, map);
    }

    public override bool Read() => this.inner.Read();

    public override Task<bool> ReadAsync(CancellationToken cancellationToken) =>
        this.inner.ReadAsync(cancellationToken);

    public override bool NextResult()
    {
        bool moved = this.inner.NextResult();
        if (moved)
        {
            this.RebindForCurrentResultSet();
        }

        return moved;
    }

    public override async Task<bool> NextResultAsync(CancellationToken cancellationToken)
    {
        bool moved = await this.inner.NextResultAsync(cancellationToken).ConfigureAwait(false);
        if (moved)
        {
            this.ScrubKeys();
            this.encryptedByOrdinal = await ResolveAsync(
                this.inner, this.metadataManager, this.dataKeyResolver, cancellationToken)
                .ConfigureAwait(false);
        }

        return moved;
    }

    public override object GetValue(int ordinal)
    {
        if (!this.encryptedByOrdinal.TryGetValue(ordinal, out EncryptedColumn? column))
        {
            return this.inner.GetValue(ordinal);
        }

        // Checked first so a SQL NULL stays DBNull rather than being fed to decryption.
        if (this.inner.IsDBNull(ordinal))
        {
            return DBNull.Value;
        }

        return this.Decrypt(ordinal, column) ?? DBNull.Value;
    }

    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, this.FieldCount);
        for (int i = 0; i < count; i++)
        {
            values[i] = this.GetValue(i);
        }

        return count;
    }

    public override bool IsDBNull(int ordinal) => this.inner.IsDBNull(ordinal);

    public override string GetName(int ordinal) => this.inner.GetName(ordinal);

    public override int GetOrdinal(string name) => this.inner.GetOrdinal(name);

    public override string GetString(int ordinal) => this.Converted<string>(ordinal);

    public override bool GetBoolean(int ordinal) => this.Converted<bool>(ordinal);

    public override byte GetByte(int ordinal) => this.Converted<byte>(ordinal);

    public override char GetChar(int ordinal) => this.Converted<char>(ordinal);

    public override DateTime GetDateTime(int ordinal) => this.Converted<DateTime>(ordinal);

    public override decimal GetDecimal(int ordinal) => this.Converted<decimal>(ordinal);

    public override double GetDouble(int ordinal) => this.Converted<double>(ordinal);

    public override float GetFloat(int ordinal) => this.Converted<float>(ordinal);

    public override Guid GetGuid(int ordinal) => this.Converted<Guid>(ordinal);

    public override short GetInt16(int ordinal) => this.Converted<short>(ordinal);

    public override int GetInt32(int ordinal) => this.Converted<int>(ordinal);

    public override long GetInt64(int ordinal) => this.Converted<long>(ordinal);

    /// <summary>
    /// Copies bytes of the value, decrypting first for an encrypted column.
    /// </summary>
    /// <remarks>
    /// A null <paramref name="buffer"/> is a request for the length rather than for data. It has to report
    /// the decrypted length: reporting the stored length would make a caller allocate for the ciphertext and
    /// read back a value padded with zeroes.
    /// </remarks>
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        if (!this.encryptedByOrdinal.ContainsKey(ordinal))
        {
            return this.inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        byte[] value = this.Converted<byte[]>(ordinal);
        if (buffer is null)
        {
            return value.Length;
        }

        long available = Math.Max(0, value.Length - dataOffset);
        int copied = (int)Math.Min(available, length);
        Array.Copy(value, dataOffset, buffer, bufferOffset, copied);
        return copied;
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        if (!this.encryptedByOrdinal.ContainsKey(ordinal))
        {
            return this.inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        char[] value = this.Converted<string>(ordinal).ToCharArray();
        if (buffer is null)
        {
            return value.Length;
        }

        long available = Math.Max(0, value.Length - dataOffset);
        int copied = (int)Math.Min(available, length);
        Array.Copy(value, dataOffset, buffer, bufferOffset, copied);
        return copied;
    }

    /// <summary>
    /// Reports the type the caller will actually receive.
    /// </summary>
    /// <remarks>
    /// For an encrypted column the stored bytes carry the type, so the answer is only known once a row is
    /// available. Before the first row, and for a NULL, <see cref="object"/> is reported - the honest answer,
    /// since nothing better can be known without decrypting.
    /// </remarks>
    public override Type GetFieldType(int ordinal)
    {
        if (!this.encryptedByOrdinal.ContainsKey(ordinal))
        {
            return this.inner.GetFieldType(ordinal);
        }

        try
        {
            object value = this.GetValue(ordinal);
            return value is DBNull ? typeof(object) : value.GetType();
        }
        catch (Exception)
        {
            return typeof(object);
        }
    }

    public override string GetDataTypeName(int ordinal) =>
        this.encryptedByOrdinal.ContainsKey(ordinal)
            ? this.GetFieldType(ordinal).Name
            : this.inner.GetDataTypeName(ordinal);

    /// <summary>
    /// Returns the inner schema with the data type of every encrypted column relaxed to
    /// <see cref="object"/>.
    /// </summary>
    /// <remarks>
    /// Without this, a schema saying the column holds bytes while the accessors hand back strings makes
    /// <c>DataTable.Load</c> fail with a type mismatch.
    /// </remarks>
    public override DataTable? GetSchemaTable()
    {
        DataTable? schema = this.inner.GetSchemaTable();
        if (schema is null || this.encryptedByOrdinal.Count == 0)
        {
            return schema;
        }

        int nameIndex = schema.Columns.IndexOf("ColumnName");
        int typeIndex = schema.Columns.IndexOf("DataType");
        if (nameIndex < 0 || typeIndex < 0)
        {
            return schema;
        }

        foreach (KeyValuePair<int, EncryptedColumn> entry in this.encryptedByOrdinal)
        {
            if (entry.Key < schema.Rows.Count)
            {
                schema.Rows[entry.Key][typeIndex] = typeof(object);
            }
        }

        return schema;
    }

    /// <summary>
    /// Enumerates this reader rather than the inner one.
    /// </summary>
    /// <remarks>
    /// Delegating to the inner reader's enumerator would hand back records that bypass decryption entirely,
    /// so every value read through <c>foreach</c> would be ciphertext.
    /// </remarks>
    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    public override void Close() => this.inner.Close();

    public override Task CloseAsync() => this.inner.CloseAsync();

    /// <summary>Forwards to the inner reader, so a caller can still reach the driver's own reader.</summary>
    /// <remarks>
    /// Entity Framework Core relies on unwrapping to the provider's reader; without this, inserting this
    /// decorator would quietly disable behaviour that depends on it.
    /// </remarks>
    public T Unwrap<T>()
        where T : class
    {
        if (this.inner is T asT)
        {
            return asT;
        }

        if (this.inner is IWrapper wrapper)
        {
            return wrapper.Unwrap<T>();
        }

        throw new ArgumentException(string.Format(
            CultureInfo.CurrentCulture,
            Resources.DecryptingDataReader_Unwrap_CannotUnwrap,
            typeof(T).Name));
    }

    public bool IsWrapperFor<T>()
        where T : class =>
        this.inner is T || (this.inner is IWrapper wrapper && wrapper.IsWrapperFor<T>());

    protected override void Dispose(bool disposing)
    {
        if (this.disposed)
        {
            return;
        }

        this.disposed = true;
        if (disposing)
        {
            this.ScrubKeys();
            this.inner.Dispose();
        }

        base.Dispose(disposing);
    }

    private static async Task<Dictionary<int, EncryptedColumn>> ResolveAsync(
        DbDataReader reader,
        MetadataManager metadataManager,
        Func<ColumnEncryptionConfig, CancellationToken, Task<byte[]>> dataKeyResolver,
        CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, EncryptedColumn>();
        if (reader.FieldCount == 0)
        {
            return map;
        }

        // The base table and column come from the result metadata, so no analysis of the query is needed and
        // joins, views, and subqueries all resolve correctly.
        IReadOnlyList<ResultColumn>? columns = DescribeColumns(reader);
        if (columns is null)
        {
            // Nothing can be identified, so nothing would be decrypted and the application would silently
            // receive stored bytes. Failing is the only safe answer.
            throw new EncryptionException(string.Format(
                CultureInfo.CurrentCulture,
                Resources.DecryptingDataReader_DescribeColumns_CannotDescribe,
                reader.GetType().Name));
        }

        foreach (ResultColumn column in columns)
        {
            ColumnEncryptionConfig? config = await column
                .IdentifyAsync(metadataManager, cancellationToken)
                .ConfigureAwait(false);

            if (config is null)
            {
                continue;
            }

            byte[] dataKey = await dataKeyResolver(config, cancellationToken).ConfigureAwait(false);
            map[column.Ordinal] = new EncryptedColumn(config, dataKey);
        }

        return map;
    }

    /// <summary>
    /// Describes the result columns, or returns <see langword="null"/> when the driver cannot describe them.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Three routes are tried, because the drivers differ. Npgsql and MySqlConnector implement
    /// <see cref="IDbColumnSchemaGenerator"/>. MySql.Data does not, but does supply the older
    /// <see cref="DbDataReader.GetSchemaTable"/>. A reader that is only a wrapper is unwrapped first, so that
    /// a decorator between this one and the driver does not hide the driver's own capability.
    /// </para>
    /// <para>
    /// Returning null rather than an empty list is deliberate: an empty list means "described, and none are
    /// encrypted", which is an ordinary result, while null means "could not be described", which is not.
    /// </para>
    /// </remarks>
    private static IReadOnlyList<ResultColumn>? DescribeColumns(DbDataReader reader)
    {
        if (Unwrapped(reader) is IDbColumnSchemaGenerator schemaGenerator)
        {
            return schemaGenerator.GetColumnSchema()
                .Where(c => c.ColumnOrdinal is int)
                .Select(c => new ResultColumn((int)c.ColumnOrdinal!, c.BaseTableName, c.BaseColumnName, c))
                .ToList();
        }

        DataTable? schema;
        try
        {
            schema = reader.GetSchemaTable();
        }
        catch (Exception)
        {
            return null;
        }

        if (schema is null)
        {
            return null;
        }

        int nameIndex = schema.Columns.IndexOf("BaseColumnName");
        int tableIndex = schema.Columns.IndexOf("BaseTableName");
        int ordinalIndex = schema.Columns.IndexOf("ColumnOrdinal");
        if (nameIndex < 0 || tableIndex < 0)
        {
            return null;
        }

        var described = new List<ResultColumn>();
        for (int i = 0; i < schema.Rows.Count; i++)
        {
            DataRow row = schema.Rows[i];
            int ordinal = ordinalIndex >= 0 && row[ordinalIndex] is int declared ? declared : i;
            described.Add(new ResultColumn(
                ordinal, row[tableIndex] as string, row[nameIndex] as string, dbColumn: null));
        }

        return described;
    }

    /// <summary>Returns the driver's own reader, looking through any decorator in between.</summary>
    private static DbDataReader Unwrapped(DbDataReader reader) =>
        reader is IWrapper wrapper && wrapper.IsWrapperFor<DbDataReader>()
            ? wrapper.Unwrap<DbDataReader>()
            : reader;

    /// <summary>One column of a result, however the driver was able to describe it.</summary>
    /// <remarks>
    /// Both description routes are reduced to this, so the identification logic below is written once. The
    /// <see cref="DbColumn"/> is carried when there was one, because the numeric identifiers PostgreSQL
    /// reports live on the driver's own subclass of it and are not part of the older schema table.
    /// </remarks>
    private sealed class ResultColumn
    {
        internal ResultColumn(int ordinal, string? baseTableName, string? baseColumnName, DbColumn? dbColumn)
        {
            this.Ordinal = ordinal;
            this.BaseTableName = baseTableName;
            this.BaseColumnName = baseColumnName;
            this.DbColumn = dbColumn;
        }

        internal int Ordinal { get; }

        private string? BaseTableName { get; }

        private string? BaseColumnName { get; }

        private DbColumn? DbColumn { get; }

        /// <summary>
        /// Works out which encrypted column, if any, this refers to.
        /// </summary>
        /// <remarks>
        /// The base table and column names are used when the driver supplies them. PostgreSQL does not: it
        /// reports the owning table and the column's position as numbers instead, which Npgsql exposes on its
        /// own column type. Those are read reflectively so that this package needs no reference to any
        /// particular driver, and the numbers are translated through the map built when the metadata was
        /// loaded.
        /// </remarks>
        internal async Task<ColumnEncryptionConfig?> IdentifyAsync(
            MetadataManager metadataManager,
            CancellationToken cancellationToken)
        {
            if (!string.IsNullOrEmpty(this.BaseTableName) && !string.IsNullOrEmpty(this.BaseColumnName))
            {
                return await metadataManager
                    .GetColumnConfigAsync(this.BaseTableName!, this.BaseColumnName!, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (this.DbColumn is null)
            {
                return null;
            }

            Type type = this.DbColumn.GetType();
            object? tableOid = type.GetProperty("TableOID")?.GetValue(this.DbColumn);
            object? attributeNumber = type.GetProperty("ColumnAttributeNumber")?.GetValue(this.DbColumn);

            if (tableOid is null || attributeNumber is null)
            {
                return null;
            }

            try
            {
                return await metadataManager.GetColumnConfigByTableOidAsync(
                    Convert.ToUInt32(tableOid, CultureInfo.InvariantCulture),
                    Convert.ToInt32(attributeNumber, CultureInfo.InvariantCulture),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
            {
                return null;
            }
        }
    }

    private void RebindForCurrentResultSet()
    {
        this.ScrubKeys();

        // NextResult is a synchronous API, so the rebuild has to complete here. The keys for columns already
        // seen are cached, so this ordinarily does no input or output.
        this.encryptedByOrdinal = ResolveAsync(
                this.inner, this.metadataManager, this.dataKeyResolver, CancellationToken.None)
            .GetAwaiter()
            .GetResult();
    }

    private void ScrubKeys()
    {
        foreach (EncryptedColumn column in this.encryptedByOrdinal.Values)
        {
            CryptographicOperations.ZeroMemory(column.DataKey);
        }

        this.encryptedByOrdinal = new Dictionary<int, EncryptedColumn>();
    }

    private T Converted<T>(int ordinal)
    {
        object value = this.GetValue(ordinal);
        if (value is T typed)
        {
            return typed;
        }

        if (value is DBNull)
        {
            throw new InvalidCastException(string.Format(
                CultureInfo.CurrentCulture,
                Resources.DecryptingDataReader_Converted_ColumnIsNull,
                this.GetName(ordinal),
                typeof(T).Name));
        }

        return (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
    }

    private object? Decrypt(int ordinal, EncryptedColumn column)
    {
        object raw = this.inner.GetValue(ordinal);
        if (raw is not byte[] stored)
        {
            throw EncryptionException.Malformed(
                $"the value in {column.Config.ColumnIdentifier} is {raw.GetType().Name} rather than bytes, so "
                + "it cannot have been written by this plugin");
        }

        return this.encryptionService.Decrypt(
            stored,
            column.DataKey,
            column.Config.DataKeyMetadata!.HmacKey,
            column.Config.Algorithm,
            column.Config.ColumnIdentifier);
    }

    /// <summary>An encrypted column of the current result set, with the key needed to read it.</summary>
    private sealed class EncryptedColumn
    {
        internal EncryptedColumn(ColumnEncryptionConfig config, byte[] dataKey)
        {
            this.Config = config;
            this.DataKey = dataKey;
        }

        internal ColumnEncryptionConfig Config { get; }

        internal byte[] DataKey { get; }
    }
}
