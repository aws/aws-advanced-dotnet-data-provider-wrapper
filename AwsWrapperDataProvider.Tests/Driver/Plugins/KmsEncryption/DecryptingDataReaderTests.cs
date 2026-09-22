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

    // Typed getters over the date and time types, which the BCL cannot convert on its own.

    /// <summary>
    /// A value stored as a timestamp comes back as a <see cref="DateTimeOffset"/>, and must still be
    /// readable with <c>GetDateTime</c>.
    /// </summary>
    /// <remarks>
    /// <see cref="DateTimeOffset"/> does not implement <see cref="IConvertible"/>, so without the conversion
    /// under test <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/> reports "Object must
    /// implement IConvertible" for a value this plugin itself wrote - a round trip through its own driver
    /// that fails.
    /// </remarks>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestTimestampIsReadableAsDateTime()
    {
        var stored = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero);

        object narrowed = DecryptingDataReader.NarrowForTypedGetter(stored, typeof(DateTime));

        Assert.Equal(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc), narrowed);
    }

    /// <summary>A date comes back as a <see cref="DateOnly"/>, and reads as midnight on that date.</summary>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestDateIsReadableAsDateTimeAtMidnight()
    {
        object narrowed = DecryptingDataReader.NarrowForTypedGetter(new DateOnly(2026, 1, 2), typeof(DateTime));

        Assert.Equal(new DateTime(2026, 1, 2, 0, 0, 0), narrowed);
    }

    /// <summary>
    /// Every date and time type reads as a string in the round-trip format, so the text does not change with
    /// the culture of the machine the application runs on.
    /// </summary>
    [Theory]
    [Trait("Category", "Unit")]
    [MemberData(nameof(RoundTripTextCases))]
    public void TestDateAndTimeTypesReadAsRoundTripText(object stored, string expected)
    {
        object narrowed = DecryptingDataReader.NarrowForTypedGetter(stored, typeof(string));

        Assert.Equal(expected, narrowed);
    }

    public static IEnumerable<object[]> RoundTripTextCases() => new List<object[]>
    {
        new object[] { new DateOnly(2026, 1, 2), "2026-01-02" },
        new object[] { new TimeOnly(3, 4, 5), "03:04:05.0000000" },
        new object[]
        {
            new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero),
            "2026-01-02T03:04:05.0000000+00:00",
        },
    };

    /// <summary>
    /// A type the conversion has no answer for is handed back unchanged, so the caller's own conversion -
    /// or its failure - is what decides.
    /// </summary>
    /// <remarks>
    /// A <see cref="TimeOnly"/> read as a <see cref="DateTime"/> is the reachable case: it carries no date,
    /// so any date returned would be invented. The reader turns this into a message naming the column and
    /// both types rather than the BCL's mention of <see cref="IConvertible"/>.
    /// </remarks>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestTimeReadAsDateTimeIsLeftAlone()
    {
        var stored = new TimeOnly(3, 4, 5);

        Assert.Equal(stored, DecryptingDataReader.NarrowForTypedGetter(stored, typeof(DateTime)));
    }

    /// <summary>
    /// Types the BCL can already convert are untouched, so the conversion adds no behaviour of its own for
    /// the types that were always fine.
    /// </summary>
    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("123-45-6789")]
    [InlineData(42)]
    [InlineData(4.5d)]
    [InlineData(true)]
    public void TestConvertibleValuesAreLeftAlone(object stored)
    {
        Assert.Same(stored, DecryptingDataReader.NarrowForTypedGetter(stored, typeof(string)));
    }

    /// <summary>
    /// A Guid is stored as its canonical text, and must read back as a Guid.
    /// </summary>
    /// <remarks>
    /// <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/> cannot produce a
    /// <see cref="Guid"/> from a string - <see cref="Guid"/> is not one of the types it handles - so without
    /// the conversion under test <c>GetGuid</c> fails on a value this plugin wrote.
    /// </remarks>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestGuidTextIsReadableAsGuid()
    {
        const string text = "00112233-4455-6677-8899-aabbccddeeff";

        object narrowed = DecryptingDataReader.NarrowForTypedGetter(text, typeof(Guid));

        Assert.Equal(Guid.Parse(text), narrowed);
    }

    /// <summary>
    /// A string that is not a Guid fails as a Guid rather than being handed back as a string, so the failure
    /// names the real problem instead of surfacing later as a cast.
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestNonGuidTextIsRejectedAsGuid()
    {
        Assert.Throws<FormatException>(
            () => DecryptingDataReader.NarrowForTypedGetter("not a guid", typeof(Guid)));
    }

    /// <summary>
    /// A string stays a string when a string is what was asked for, so adding the Guid conversion did not
    /// change the ordinary case.
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestGuidTextStaysTextWhenReadAsString()
    {
        const string text = "00112233-4455-6677-8899-aabbccddeeff";

        Assert.Same(text, DecryptingDataReader.NarrowForTypedGetter(text, typeof(string)));
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
