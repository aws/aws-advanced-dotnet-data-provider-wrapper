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

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Parser;

/// <summary>
/// What one statement in a command was found to do, as far as encryption is concerned.
/// </summary>
/// <remarks>
/// This records only what the statement text says. Whether any of the columns are actually encrypted is
/// decided later, against the encryption metadata.
/// </remarks>
internal class QueryAnalysis
{
    internal QueryAnalysis(
        string? table,
        IReadOnlyDictionary<string, string> writtenColumnsByParameter,
        IReadOnlyDictionary<string, string> predicateColumnsByParameter,
        IReadOnlyList<string> columnsWrittenWithoutAParameter,
        IReadOnlyList<string> refusals)
    {
        this.Table = table;
        this.WrittenColumnsByParameter = writtenColumnsByParameter;
        this.PredicateColumnsByParameter = predicateColumnsByParameter;
        this.ColumnsWrittenWithoutAParameter = columnsWrittenWithoutAParameter;
        this.UnreadableReasons = refusals;
    }

    /// <summary>Gets the table being written to, or <see langword="null"/> if the statement is not a write.</summary>
    internal string? Table { get; }

    /// <summary>
    /// Gets the column each parameter supplies a stored value for, keyed by normalised parameter name.
    /// </summary>
    internal IReadOnlyDictionary<string, string> WrittenColumnsByParameter { get; }

    /// <summary>
    /// Gets the column each parameter is compared against in a predicate, keyed by normalised parameter
    /// name. A parameter compared against an encrypted column has to be refused: every value is encrypted
    /// with a fresh single-use number, so the ciphertext never repeats and the comparison matches nothing.
    /// </summary>
    internal IReadOnlyDictionary<string, string> PredicateColumnsByParameter { get; }

    /// <summary>
    /// Gets the columns given a value that is not a plain parameter - a literal, <c>DEFAULT</c>, or an
    /// expression such as <c>now()</c>. These are harmless unless the column turns out to be encrypted, in
    /// which case the statement must be refused: the plugin has nothing to encrypt, so the column would be
    /// left readable.
    /// </summary>
    internal IReadOnlyList<string> ColumnsWrittenWithoutAParameter { get; }

    /// <summary>
    /// Gets the reasons this statement cannot be handled, independently of the metadata. Empty when the
    /// statement was understood.
    /// </summary>
    internal IReadOnlyList<string> UnreadableReasons { get; }

    /// <summary>Gets a value indicating whether the statement writes nothing this plugin cares about.</summary>
    internal bool IsIrrelevant =>
        this.Table is null
        && this.UnreadableReasons.Count == 0
        && this.WrittenColumnsByParameter.Count == 0
        && this.PredicateColumnsByParameter.Count == 0;
}
