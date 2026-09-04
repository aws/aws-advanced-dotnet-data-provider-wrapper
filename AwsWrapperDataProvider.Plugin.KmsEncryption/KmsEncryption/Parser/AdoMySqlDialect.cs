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
/// The MySQL grammar, taught that <c>@name</c> begins an identifier.
/// </summary>
/// <remarks>
/// MySQL already accepts <c>@</c> as an identifier start, for its user variables, so this exists to state
/// the intent explicitly rather than to rely on that coincidence continuing to hold.
/// </remarks>
internal sealed class AdoMySqlDialect : MySqlDialect
{
    internal static readonly AdoMySqlDialect Instance = new();

    public override bool IsIdentifierStart(char character) =>
        character == '@' || base.IsIdentifierStart(character);
}
