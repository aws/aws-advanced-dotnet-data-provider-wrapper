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

using SqlParser.Dialects;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Parser;

/// <summary>
/// The PostgreSQL grammar, taught that <c>@name</c> begins an identifier.
/// </summary>
/// <remarks>
/// <para>
/// <c>@name</c> is an ADO.NET convention, not PostgreSQL syntax - the driver rewrites it into a positional
/// placeholder before the server sees it. A parser faithful to PostgreSQL therefore reads <c>@id</c> as its
/// prefix absolute-value operator applied to a column named <c>id</c>, which is correct about PostgreSQL
/// and useless here: the placeholder disappears into an arithmetic expression over a column reference.
/// </para>
/// <para>
/// Treating <c>@</c> as the start of an identifier makes <c>@id</c> arrive as a single identifier whose text
/// carries the marker, which is how the MySQL and SQL Server grammars in this parser already behave. The
/// cost is that PostgreSQL's <c>@</c> operator can no longer be written unspaced before an identifier; for a
/// driver whose callers write <c>@name</c> to mean a parameter, that is the right way round.
/// </para>
/// </remarks>
internal sealed class AdoPostgreSqlDialect : PostgreSqlDialect
{
    internal static readonly AdoPostgreSqlDialect Instance = new();

    public override bool IsIdentifierStart(char character) =>
        character == '@' || base.IsIdentifierStart(character);
}
