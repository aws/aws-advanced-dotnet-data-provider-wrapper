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

/// <summary>Chooses the grammar to read a statement with.</summary>
internal static class AdoDialect
{
    /// <summary>
    /// Returns the grammar for the connected engine. PostgreSQL is the answer when the engine is not MySQL,
    /// including when it is not yet known, because its grammar is the stricter of the two about the quoting
    /// that would otherwise be misread.
    /// </summary>
    internal static Dialect For(bool mySql) =>
        mySql ? AdoMySqlDialect.Instance : AdoPostgreSqlDialect.Instance;
}
