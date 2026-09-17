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

using System.Buffers.Binary;
using System.Globalization;
using System.Text;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

/// <summary>
/// Converts values to and from the byte layout that is encrypted, and back again.
/// </summary>
/// <remarks>
/// <para>
/// <b>Numbers are always big-endian.</b> Never use <see cref="BitConverter"/> here: it follows the
/// processor's byte order, which is little-endian on x86 and Arm, whereas this format is big-endian.
/// Getting it wrong is silent rather than loud - the integrity tag still verifies, decryption still
/// succeeds, and the length is still correct, so the only symptom is a plausible but wrong number
/// (1234567 reads back as -2016013824).
/// </para>
/// <para>
/// <b>Text is always invariant-culture.</b> A machine configured with a comma decimal separator would
/// otherwise write values that no other machine could read back.
/// </para>
/// </remarks>
internal static class ValueSerializer
{
    private static readonly DateTimeOffset UnixEpoch = DateTimeOffset.UnixEpoch;

    /// <summary>Returns the marker describing how <paramref name="value"/> will be serialized.</summary>
    /// <exception cref="EncryptionException">Thrown for a type that cannot be encrypted.</exception>
    internal static TypeMarker MarkerFor(object value) => value switch
    {
        string => TypeMarker.String,
        int => TypeMarker.Integer,
        long => TypeMarker.Long,
        double => TypeMarker.Double,
        float => TypeMarker.Float,
        bool => TypeMarker.Boolean,
        decimal => TypeMarker.BigDecimal,
        DateTimeOffset => TypeMarker.Timestamp,
        DateOnly => TypeMarker.LocalDate,
        TimeOnly => TypeMarker.LocalTime,
        DateTime => TypeMarker.LocalDateTime,
        byte[] => TypeMarker.ByteArray,
        _ => throw EncryptionException.UnsupportedType(value.GetType()),
    };

    /// <summary>Converts a value to the bytes that will be encrypted.</summary>
    internal static byte[] Serialize(object value)
    {
        switch (value)
        {
            case string s:
                return Encoding.UTF8.GetBytes(s);

            case int i:
                byte[] intBytes = new byte[sizeof(int)];
                BinaryPrimitives.WriteInt32BigEndian(intBytes, i);
                return intBytes;

            case long l:
                return Int64Bytes(l);

            case double d:
                byte[] doubleBytes = new byte[sizeof(double)];
                BinaryPrimitives.WriteDoubleBigEndian(doubleBytes, d);
                return doubleBytes;

            case float f:
                byte[] floatBytes = new byte[sizeof(float)];
                BinaryPrimitives.WriteSingleBigEndian(floatBytes, f);
                return floatBytes;

            case bool b:
                return new[] { b ? (byte)1 : (byte)0 };

            case decimal m:
                return Encoding.UTF8.GetBytes(m.ToString(CultureInfo.InvariantCulture));

            case DateTimeOffset dto:
                return Int64Bytes(dto.ToUnixTimeMilliseconds());

            case DateOnly dateOnly:
                return Encoding.UTF8.GetBytes(dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));

            case TimeOnly timeOnly:
                return Encoding.UTF8.GetBytes(FormatIsoTime(timeOnly));

            case DateTime dt:
                return Encoding.UTF8.GetBytes(FormatIsoDateTime(dt));

            case byte[] bytes:
                return bytes;

            default:
                throw EncryptionException.UnsupportedType(value.GetType());
        }
    }

    /// <summary>Rebuilds a value from decrypted bytes according to <paramref name="marker"/>.</summary>
    internal static object Deserialize(byte[] data, TypeMarker marker)
    {
        switch (marker)
        {
            case TypeMarker.String:
                return Encoding.UTF8.GetString(data);

            case TypeMarker.Integer:
                RequireLength(data, sizeof(int), marker);
                return BinaryPrimitives.ReadInt32BigEndian(data);

            case TypeMarker.Long:
                RequireLength(data, sizeof(long), marker);
                return BinaryPrimitives.ReadInt64BigEndian(data);

            case TypeMarker.Double:
                RequireLength(data, sizeof(double), marker);
                return BinaryPrimitives.ReadDoubleBigEndian(data);

            case TypeMarker.Float:
                RequireLength(data, sizeof(float), marker);
                return BinaryPrimitives.ReadSingleBigEndian(data);

            case TypeMarker.Boolean:
                RequireLength(data, 1, marker);
                return data[0] != 0;

            case TypeMarker.BigDecimal:
                return ParseDecimal(Encoding.UTF8.GetString(data));

            case TypeMarker.Timestamp:
                RequireLength(data, sizeof(long), marker);
                return DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64BigEndian(data));

            // Date and Time are epoch milliseconds written by the AWS Advanced JDBC Wrapper. Narrow them to
            // matching .NET type so a caller sees a date or a time rather than a full instant.
            case TypeMarker.Date:
                RequireLength(data, sizeof(long), marker);
                return DateOnly.FromDateTime(
                    DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64BigEndian(data)).UtcDateTime);

            case TypeMarker.Time:
                RequireLength(data, sizeof(long), marker);
                return TimeOnly.FromDateTime(
                    DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64BigEndian(data)).UtcDateTime);

            case TypeMarker.LocalDate:
                return ParseDateOnly(Encoding.UTF8.GetString(data));

            case TypeMarker.LocalTime:
                return ParseTimeOnly(Encoding.UTF8.GetString(data));

            case TypeMarker.LocalDateTime:
                return ParseDateTime(Encoding.UTF8.GetString(data));

            case TypeMarker.ByteArray:
                return data;

            case TypeMarker.Generic:
                throw EncryptionException.Malformed(
                    "the value was written with the generic type marker, which has no defined layout");

            default:
                throw EncryptionException.Malformed(
                    FormattableString.Invariant($"unknown type marker {(byte)marker}"));
        }
    }

    private static byte[] Int64Bytes(long value)
    {
        byte[] bytes = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        return bytes;
    }

    /// <summary>
    /// Formats a time as the shortest ISO-8601 form that is still exact, matching how the AWS Advanced JDBC
    /// Wrapper writes it: seconds are omitted when zero, and fractional digits appear in groups of three
    /// only when non-zero.
    /// </summary>
    private static string FormatIsoTime(TimeOnly value)
    {
        var inv = CultureInfo.InvariantCulture;
        if (value.Millisecond != 0 || HasSubMillisecond(value))
        {
            // Seven digits covers the full precision of a .NET tick; trailing zeros are trimmed so a
            // whole number of milliseconds is written with three digits, as Java does.
            string fraction = value.ToString("fffffff", inv).TrimEnd('0');
            return value.ToString("HH:mm:ss", inv) + "." + fraction;
        }

        return value.Second != 0
            ? value.ToString("HH:mm:ss", inv)
            : value.ToString("HH:mm", inv);
    }

    private static string FormatIsoDateTime(DateTime value)
    {
        var inv = CultureInfo.InvariantCulture;
        return value.ToString("yyyy-MM-dd", inv) + "T" + FormatIsoTime(TimeOnly.FromDateTime(value));
    }

    private static bool HasSubMillisecond(TimeOnly value) =>
        value.Ticks % TimeSpan.TicksPerMillisecond != 0;

    private static decimal ParseDecimal(string text)
    {
        // AllowExponent is required: these values may originate from Java's BigDecimal, whose text form
        // can be scientific (for example "1E+3").
        if (!decimal.TryParse(
                text,
                NumberStyles.Float,
                CultureInfo.InvariantCulture,
                out decimal value))
        {
            throw EncryptionException.Malformed(
                "the stored decimal could not be read as a .NET decimal; it may exceed the range or "
                + "precision that decimal supports");
        }

        return value;
    }

    private static DateOnly ParseDateOnly(string text) =>
        DateOnly.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateOnly value)
            ? value
            : throw EncryptionException.Malformed("the stored date is not a valid ISO-8601 date");

    private static TimeOnly ParseTimeOnly(string text) =>
        TimeOnly.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out TimeOnly value)
            ? value
            : throw EncryptionException.Malformed("the stored time is not a valid ISO-8601 time");

    private static DateTime ParseDateTime(string text) =>
        DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime value)
            ? value
            : throw EncryptionException.Malformed("the stored timestamp is not a valid ISO-8601 date and time");

    private static void RequireLength(byte[] data, int expected, TypeMarker marker)
    {
        if (data.Length != expected)
        {
            throw EncryptionException.Malformed(
                FormattableString.Invariant(
                    $"a {marker} value must be {expected} bytes but was {data.Length}"));
        }
    }
}
