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

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

/// <summary>
/// Identifies the type of a value inside an encrypted payload, so that decryption can rebuild the
/// original type. Stored as a single byte immediately after the integrity tag.
/// <para>
/// <b>These numbers are part of the stored data format and must never change.</b> They also match the
/// markers the AWS Advanced JDBC Wrapper writes, so a column encrypted by either driver can be read by the
/// other. Some markers therefore describe Java types with no exact .NET equivalent; those are readable
/// here and map onto the closest .NET type, but are never written.
/// </para>
/// </summary>
internal enum TypeMarker : byte
{
    /// <summary>UTF-8 text. Written for <see cref="string"/>.</summary>
    String = 1,

    /// <summary>Four-byte big-endian signed integer. Written for <see cref="int"/>.</summary>
    Integer = 2,

    /// <summary>Eight-byte big-endian signed integer. Written for <see cref="long"/>.</summary>
    Long = 3,

    /// <summary>Eight-byte big-endian IEEE-754. Written for <see cref="double"/>.</summary>
    Double = 4,

    /// <summary>Four-byte big-endian IEEE-754. Written for <see cref="float"/>.</summary>
    Float = 5,

    /// <summary>Single byte, 0 or 1. Written for <see cref="bool"/>.</summary>
    Boolean = 6,

    /// <summary>
    /// Decimal rendered as invariant-culture text. Written for <see cref="decimal"/>. A value from the
    /// AWS Advanced JDBC Wrapper comes from Java's arbitrary-precision <c>BigDecimal</c>, so it may use
    /// scientific notation and may exceed the range of <see cref="decimal"/>.
    /// </summary>
    BigDecimal = 7,

    /// <summary>
    /// Eight-byte big-endian Unix epoch milliseconds, date only. Read-only: written by the AWS Advanced
    /// JDBC Wrapper for <c>java.sql.Date</c>. This driver writes <see cref="LocalDate"/> for a date.
    /// </summary>
    Date = 8,

    /// <summary>
    /// Eight-byte big-endian Unix epoch milliseconds, time only. Read-only: written by the AWS Advanced
    /// JDBC Wrapper for <c>java.sql.Time</c>. This driver writes <see cref="LocalTime"/> for a time.
    /// </summary>
    Time = 9,

    /// <summary>
    /// Eight-byte big-endian Unix epoch milliseconds. Written for <see cref="DateTimeOffset"/>, which
    /// is the only .NET type here that carries an unambiguous instant.
    /// </summary>
    Timestamp = 10,

    /// <summary>ISO-8601 date text, <c>yyyy-MM-dd</c>. Written for <see cref="DateOnly"/>.</summary>
    LocalDate = 11,

    /// <summary>
    /// ISO-8601 time text. Written for <see cref="TimeOnly"/>. The AWS Advanced JDBC Wrapper emits the
    /// shortest form that is still exact (<c>10:15</c>, <c>10:15:30</c>, <c>10:15:30.123</c>), and this
    /// driver does the same, so both produce identical text for identical values.
    /// </summary>
    LocalTime = 12,

    /// <summary>
    /// ISO-8601 local date and time text. Written for <see cref="DateTime"/>. Carries no offset, so a
    /// value round-trips as wall-clock time rather than as an instant.
    /// </summary>
    LocalDateTime = 13,

    /// <summary>Raw bytes, stored verbatim. Written for <see cref="byte"/> arrays.</summary>
    ByteArray = 14,

    /// <summary>
    /// Reserved by the AWS Advanced JDBC Wrapper for values it could not classify. Never written by this
    /// driver, and rejected on read because the payload has no defined layout.
    /// </summary>
    Generic = 99,
}
