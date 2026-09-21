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
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Wrapper;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.KmsEncryption;

/// <summary>
/// Covers how result columns are located, which is the step that decides whether a value is decrypted at
/// all.
/// </summary>
/// <remarks>
/// <para>
/// The ordinal a column is filed under has to be the ordinal the application reads with. Getting it wrong
/// is silent in the worst way: a single-column query is handed the stored ciphertext with no error, and a
/// wider one tries to decrypt the neighbouring column instead.
/// </para>
/// <para>
/// These are unit tests because the integration and local suites cannot reach this path. Npgsql and
/// MySqlConnector implement <see cref="IDbColumnSchemaGenerator"/>, so they take a different route
/// entirely; only MySql.Data falls back to <see cref="DbDataReader.GetSchemaTable"/>, and no suite that
/// runs against a real cluster uses that driver.
/// </para>
/// </remarks>
public class DecryptingDataReaderTests
{
    /// <summary>
    /// A schema table numbered from zero, as the ADO.NET convention says it should be.
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestZeroBasedSchemaTableMapsToReaderOrdinals()
    {
        var reader = new SchemaTableOnlyReader(firstOrdinal: 0, "ssn", "name", "email");

        IReadOnlyList<DecryptingDataReader.ResultColumn>? described =
            DecryptingDataReader.DescribeColumns(reader);

        Assert.NotNull(described);
        Assert.Equal(new[] { 0, 1, 2 }, described!.Select(c => c.Ordinal));
    }

    /// <summary>
    /// A schema table numbered from one, which is what MySql.Data reports.
    /// </summary>
    /// <remarks>
    /// Believing the driver here shifts every column one place up: the encrypted column at reader ordinal 0
    /// gets filed under 1, so nothing decrypts and the caller receives raw bytes. The row's position in the
    /// table is used instead, because the rows are in column order by contract and so cannot disagree.
    /// </remarks>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestOneBasedSchemaTableStillMapsToReaderOrdinals()
    {
        var reader = new SchemaTableOnlyReader(firstOrdinal: 1, "ssn", "name", "email");

        IReadOnlyList<DecryptingDataReader.ResultColumn>? described =
            DecryptingDataReader.DescribeColumns(reader);

        Assert.NotNull(described);
        Assert.Equal(new[] { 0, 1, 2 }, described!.Select(c => c.Ordinal));
    }

    /// <summary>
    /// An ordinal column that is missing entirely is no obstacle, since it is not consulted.
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestSchemaTableWithNoOrdinalColumnIsStillDescribed()
    {
        var reader = new SchemaTableOnlyReader(firstOrdinal: null, "ssn", "name");

        IReadOnlyList<DecryptingDataReader.ResultColumn>? described =
            DecryptingDataReader.DescribeColumns(reader);

        Assert.NotNull(described);
        Assert.Equal(new[] { 0, 1 }, described!.Select(c => c.Ordinal));
    }

    /// <summary>
    /// Without the base table and column names there is nothing to match against the metadata, so the
    /// result is "could not be described" rather than "described, and none are encrypted".
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestSchemaTableWithoutBaseNamesCannotBeDescribed()
    {
        var reader = new SchemaTableOnlyReader(firstOrdinal: 0, "ssn") { OmitBaseNames = true };

        Assert.Null(DecryptingDataReader.DescribeColumns(reader));
    }

    /// <summary>A driver that cannot describe its columns at all is reported as such.</summary>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestNullSchemaTableCannotBeDescribed()
    {
        var reader = new SchemaTableOnlyReader(firstOrdinal: 0) { ReturnNullSchema = true };

        Assert.Null(DecryptingDataReader.DescribeColumns(reader));
    }

    /// <summary>A driver that throws from GetSchemaTable is reported as unable to describe, not fatal.</summary>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestThrowingSchemaTableCannotBeDescribed()
    {
        var reader = new SchemaTableOnlyReader(firstOrdinal: 0) { ThrowFromSchema = true };

        Assert.Null(DecryptingDataReader.DescribeColumns(reader));
    }

    /// <summary>
    /// A reader that offers only <see cref="DbDataReader.GetSchemaTable"/>, as MySql.Data does.
    /// </summary>
    /// <remarks>
    /// Deliberately does not implement <see cref="IDbColumnSchemaGenerator"/>, so that the fallback route is
    /// the one under test. Everything other than the schema table throws, because nothing else is reached.
    /// </remarks>
    private sealed class SchemaTableOnlyReader : DbDataReader
    {
        private readonly int? firstOrdinal;
        private readonly string[] columns;

        internal SchemaTableOnlyReader(int? firstOrdinal, params string[] columns)
        {
            this.firstOrdinal = firstOrdinal;
            this.columns = columns;
        }

        internal bool OmitBaseNames { get; init; }

        internal bool ReturnNullSchema { get; init; }

        internal bool ThrowFromSchema { get; init; }

        public override int FieldCount => this.columns.Length;

        public override int Depth => 0;

        public override bool HasRows => false;

        public override bool IsClosed => false;

        public override int RecordsAffected => 0;

        public override object this[int ordinal] => throw new NotSupportedException();

        public override object this[string name] => throw new NotSupportedException();

        public override DataTable? GetSchemaTable()
        {
            if (this.ThrowFromSchema)
            {
                throw new NotSupportedException("this driver does not describe its columns");
            }

            if (this.ReturnNullSchema)
            {
                return null;
            }

            var schema = new DataTable();
            schema.Columns.Add("ColumnName", typeof(string));
            if (this.firstOrdinal is not null)
            {
                schema.Columns.Add("ColumnOrdinal", typeof(int));
            }

            if (!this.OmitBaseNames)
            {
                schema.Columns.Add("BaseTableName", typeof(string));
                schema.Columns.Add("BaseColumnName", typeof(string));
            }

            for (int i = 0; i < this.columns.Length; i++)
            {
                DataRow row = schema.NewRow();
                row["ColumnName"] = this.columns[i];
                if (this.firstOrdinal is not null)
                {
                    row["ColumnOrdinal"] = this.firstOrdinal.Value + i;
                }

                if (!this.OmitBaseNames)
                {
                    row["BaseTableName"] = "users";
                    row["BaseColumnName"] = this.columns[i];
                }

                schema.Rows.Add(row);
            }

            return schema;
        }

        public override bool GetBoolean(int ordinal) => throw new NotSupportedException();

        public override byte GetByte(int ordinal) => throw new NotSupportedException();

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) =>
            throw new NotSupportedException();

        public override char GetChar(int ordinal) => throw new NotSupportedException();

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) =>
            throw new NotSupportedException();

        public override string GetDataTypeName(int ordinal) => throw new NotSupportedException();

        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();

        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();

        public override double GetDouble(int ordinal) => throw new NotSupportedException();

        public override IEnumerator GetEnumerator() => throw new NotSupportedException();

        public override Type GetFieldType(int ordinal) => throw new NotSupportedException();

        public override float GetFloat(int ordinal) => throw new NotSupportedException();

        public override Guid GetGuid(int ordinal) => throw new NotSupportedException();

        public override short GetInt16(int ordinal) => throw new NotSupportedException();

        public override int GetInt32(int ordinal) => throw new NotSupportedException();

        public override long GetInt64(int ordinal) => throw new NotSupportedException();

        public override string GetName(int ordinal) => this.columns[ordinal];

        public override int GetOrdinal(string name) => Array.IndexOf(this.columns, name);

        public override string GetString(int ordinal) => throw new NotSupportedException();

        public override object GetValue(int ordinal) => throw new NotSupportedException();

        public override int GetValues(object[] values) => throw new NotSupportedException();

        public override bool IsDBNull(int ordinal) => throw new NotSupportedException();

        public override bool NextResult() => false;

        public override bool Read() => false;
    }
}
