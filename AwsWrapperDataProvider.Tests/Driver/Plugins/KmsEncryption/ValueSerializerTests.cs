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

using System.Globalization;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.KmsEncryption;

/// <summary>
/// Locks the serialized byte layout of every supported type.
/// <para>
/// The expected values below are not hand-computed: they were produced by running the AWS Advanced JDBC
/// Wrapper's own serialization for the same inputs, so any drift here means a column written by one
/// driver can no longer be read by the other. Treat a failure as a compatibility break, not as a test
/// that needs updating.
/// </para>
/// </summary>
public class ValueSerializerTests
{
    [Theory]
    [Trait("Category", "Unit")]
    // Text is UTF-8 and therefore byte-order independent.
    [InlineData("123-45-6789", (byte)1, "3132332d34352d36373839")]
    // Numbers are big-endian. Little-endian would give 87d61200 here, which decrypts without any error
    // and yields -2016013824 instead of 1234567.
    [InlineData(1234567, (byte)2, "0012d687")]
    [InlineData(1234567890123L, (byte)3, "0000011f71fb04cb")]
    [InlineData(1234.5678d, (byte)4, "40934a456d5cfaad")]
    [InlineData(12.34f, (byte)5, "414570a4")]
    [InlineData(true, (byte)6, "01")]
    [InlineData(false, (byte)6, "00")]
    public void TestSerializedLayoutMatchesTheStoredFormat(object value, byte expectedMarker, string expectedHex)
    {
        Assert.Equal((TypeMarker)expectedMarker, ValueSerializer.MarkerFor(value));
        Assert.Equal(expectedHex, Convert.ToHexString(ValueSerializer.Serialize(value)).ToLowerInvariant());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestDecimalKeepsTrailingZerosAndUsesInvariantFormatting()
    {
        // "123.4500" not "123.45": the scale is significant, and a comma-decimal machine must still
        // produce a dot.
        Assert.Equal(
            "3132332e34353030",
            Convert.ToHexString(ValueSerializer.Serialize(123.4500m)).ToLowerInvariant());
        Assert.Equal(TypeMarker.BigDecimal, ValueSerializer.MarkerFor(123.4500m));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestTimestampIsEpochMillisecondsBigEndian()
    {
        var value = DateTimeOffset.FromUnixTimeMilliseconds(1767225600000L);
        Assert.Equal(TypeMarker.Timestamp, ValueSerializer.MarkerFor(value));
        Assert.Equal(
            "0000019b76daa800",
            Convert.ToHexString(ValueSerializer.Serialize(value)).ToLowerInvariant());
    }

    [Theory]
    [Trait("Category", "Unit")]
    // The shortest exact ISO-8601 form: seconds omitted when zero, fractions only when non-zero.
    // A plain "HH:mm:ss" format string would emit "10:15:00" and break compatibility.
    [InlineData(10, 15, 0, 0, "10:15")]
    [InlineData(10, 15, 30, 0, "10:15:30")]
    [InlineData(10, 15, 30, 123, "10:15:30.123")]
    [InlineData(0, 0, 0, 0, "00:00")]
    public void TestTimeUsesShortestExactIsoForm(int h, int m, int s, int ms, string expected)
    {
        var value = new TimeOnly(h, m, s, ms);
        Assert.Equal(TypeMarker.LocalTime, ValueSerializer.MarkerFor(value));
        Assert.Equal(expected, System.Text.Encoding.UTF8.GetString(ValueSerializer.Serialize(value)));
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(2026, 1, 1, 10, 15, 0, "2026-01-01T10:15")]
    [InlineData(2026, 1, 1, 10, 15, 30, "2026-01-01T10:15:30")]
    public void TestDateTimeUsesShortestExactIsoForm(
        int y, int mo, int d, int h, int mi, int s, string expected)
    {
        var value = new DateTime(y, mo, d, h, mi, s, DateTimeKind.Unspecified);
        Assert.Equal(TypeMarker.LocalDateTime, ValueSerializer.MarkerFor(value));
        Assert.Equal(expected, System.Text.Encoding.UTF8.GetString(ValueSerializer.Serialize(value)));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestDateIsIsoText()
    {
        var value = new DateOnly(2026, 1, 1);
        Assert.Equal(TypeMarker.LocalDate, ValueSerializer.MarkerFor(value));
        Assert.Equal(
            "323032362d30312d3031",
            Convert.ToHexString(ValueSerializer.Serialize(value)).ToLowerInvariant());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestByteArrayIsStoredVerbatim()
    {
        byte[] value = { 0xDE, 0xAD, 0xBE, 0xEF };
        Assert.Equal(TypeMarker.ByteArray, ValueSerializer.MarkerFor(value));
        Assert.Equal("deadbeef", Convert.ToHexString(ValueSerializer.Serialize(value)).ToLowerInvariant());
    }

    [Theory]
    [Trait("Category", "Unit")]
    // Values written by the other driver's arbitrary-precision decimal may be scientific.
    [InlineData("1E+3", "1000")]
    [InlineData("1.5E-3", "0.0015")]
    [InlineData("123.4500", "123.4500")]
    public void TestDecimalReadsScientificNotationWrittenByTheOtherDriver(string stored, string expected)
    {
        object value = ValueSerializer.Deserialize(
            System.Text.Encoding.UTF8.GetBytes(stored), TypeMarker.BigDecimal);
        Assert.Equal(expected, ((decimal)value).ToString(CultureInfo.InvariantCulture));
    }

    [Theory]
    [Trait("Category", "Unit")]
    // Forms the other driver can emit that a strict parser would reject.
    [InlineData("10:15", 10, 15, 0)]
    [InlineData("10:15:30", 10, 15, 30)]
    public void TestTimeReadsShortFormsWrittenByTheOtherDriver(string stored, int h, int m, int s)
    {
        object value = ValueSerializer.Deserialize(
            System.Text.Encoding.UTF8.GetBytes(stored), TypeMarker.LocalTime);
        Assert.Equal(new TimeOnly(h, m, s), (TimeOnly)value);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestEpochMillisecondMarkersFromTheOtherDriverNarrowToDateAndTime()
    {
        // java.sql.Date / java.sql.Time are written as epoch milliseconds; this driver never writes
        // these markers but must read them.
        object date = ValueSerializer.Deserialize(
            Convert.FromHexString("0000000005265c00"), TypeMarker.Date);
        Assert.Equal(new DateOnly(1970, 1, 2), (DateOnly)date);

        object time = ValueSerializer.Deserialize(
            Convert.FromHexString("000000000037dcc8"), TypeMarker.Time);
        Assert.Equal(new TimeOnly(1, 1, 1), (TimeOnly)time);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestGenericMarkerIsRejected()
    {
        Assert.Throws<EncryptionException>(
            () => ValueSerializer.Deserialize(new byte[] { 1 }, TypeMarker.Generic));
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData((byte)2, 3)] // Integer
    [InlineData((byte)3, 4)] // Long
    [InlineData((byte)4, 7)] // Double
    [InlineData((byte)6, 2)] // Boolean
    public void TestWrongLengthForFixedWidthTypeIsRejected(byte marker, int length)
    {
        Assert.Throws<EncryptionException>(
            () => ValueSerializer.Deserialize(new byte[length], (TypeMarker)marker));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestUnsupportedTypeIsRejected()
    {
        Assert.Throws<EncryptionException>(() => ValueSerializer.MarkerFor(Guid.NewGuid()));
    }
}
