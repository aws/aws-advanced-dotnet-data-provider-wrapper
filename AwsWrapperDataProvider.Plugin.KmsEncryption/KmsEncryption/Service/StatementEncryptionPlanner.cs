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

using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Metadata;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Parser;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Wrapper;
using SqlParser;
using SqlParser.Tokens;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

/// <summary>
/// Decides what a statement requires, by combining what <see cref="SqlWriteScanner"/> found in the text
/// with what the encryption metadata says is encrypted.
/// </summary>
/// <remarks>
/// A statement the scanner could not model is only refused when it touches a table that has encrypted
/// columns. Statements elsewhere in the schema - however complicated - pass through untouched.
/// </remarks>
internal sealed class StatementEncryptionPlanner : IStatementEncryptionPlanner
{
    private readonly MetadataManager metadataManager;

    internal StatementEncryptionPlanner(MetadataManager metadataManager)
    {
        this.metadataManager = metadataManager;
    }

    public async Task<StatementEncryptionPlan> PlanAsync(string commandText, CancellationToken cancellationToken)
    {
        List<QueryAnalysis> statements = SqlWriteScanner.Scan(commandText, this.metadataManager.IsMySql);

        var toEncrypt = new Dictionary<string, ColumnEncryptionConfig>(StringComparer.Ordinal);
        var limitations = new List<EncryptionLimitation>();

        foreach (QueryAnalysis statement in statements)
        {
            if (statement.UnreadableReasons.Count > 0)
            {
                // The scanner did not understand the statement. Whether that matters depends on whether an
                // encrypted table is involved, and without a table name that cannot be established - so the
                // statement is refused only if some table in the schema is encrypted and this text mentions
                // it. Since the table is unknown here, refuse conservatively when any encryption is
                // configured at all.
                if (await this.AnyEncryptedTableMentionedAsync(commandText, cancellationToken)
                    .ConfigureAwait(false))
                {
                    foreach (string reason in statement.UnreadableReasons)
                    {
                        limitations.Add(new EncryptionLimitation("statement", reason));
                    }
                }

                continue;
            }

            if (statement.Table is null)
            {
                continue;
            }

            if (!await this.metadataManager
                    .HasEncryptedColumnsAsync(statement.Table, cancellationToken)
                    .ConfigureAwait(false))
            {
                continue;
            }

            await this.PlanStatementAsync(statement, toEncrypt, limitations, cancellationToken)
                .ConfigureAwait(false);
        }

        if (toEncrypt.Count == 0 && limitations.Count == 0)
        {
            return StatementEncryptionPlan.None;
        }

        return new StatementEncryptionPlan(toEncrypt, limitations);
    }

    private async Task PlanStatementAsync(
        QueryAnalysis statement,
        Dictionary<string, ColumnEncryptionConfig> toEncrypt,
        List<EncryptionLimitation> limitations,
        CancellationToken cancellationToken)
    {
        string table = statement.Table!;

        foreach (KeyValuePair<string, string> pair in statement.WrittenColumnsByParameter)
        {
            ColumnEncryptionConfig? column = await this.metadataManager
                .GetColumnConfigAsync(table, pair.Value, cancellationToken)
                .ConfigureAwait(false);

            if (column is not null)
            {
                toEncrypt[ParameterSubstitution.NormaliseName(pair.Key)] = column;
            }
        }

        // A value that is not a plain parameter cannot be encrypted, so if its column is encrypted the
        // statement would store readable data.
        foreach (string columnName in statement.ColumnsWrittenWithoutAParameter)
        {
            ColumnEncryptionConfig? column = await this.metadataManager
                .GetColumnConfigAsync(table, columnName, cancellationToken)
                .ConfigureAwait(false);

            if (column is not null)
            {
                limitations.Add(new EncryptionLimitation(
                    column.ColumnIdentifier,
                    "the value written to this encrypted column is a literal or an expression rather than a "
                    + "parameter, so it cannot be encrypted; bind it as a parameter instead"));
            }
        }

        // A parameter compared against an encrypted column can never match: every value is encrypted with a
        // fresh single-use number, so the same input never produces the same stored bytes.
        foreach (KeyValuePair<string, string> pair in statement.PredicateColumnsByParameter)
        {
            ColumnEncryptionConfig? column = await this.metadataManager
                .GetColumnConfigAsync(table, pair.Value, cancellationToken)
                .ConfigureAwait(false);

            if (column is not null)
            {
                limitations.Add(new EncryptionLimitation(
                    pair.Key,
                    $"it is compared against the encrypted column {column.ColumnIdentifier}, which cannot "
                    + "match any row because each value is encrypted with a different single-use number"));
            }
        }
    }

    /// <summary>
    /// Returns whether the command text mentions any table that has encrypted columns. Used to decide
    /// whether a statement the scanner could not model needs to be refused.
    /// </summary>
    private async Task<bool> AnyEncryptedTableMentionedAsync(
        string commandText,
        CancellationToken cancellationToken)
    {
        IList<Token>? tokens = TryTokenize(commandText, this.metadataManager.IsMySql);
        if (tokens is null)
        {
            // The text cannot even be split into words, so an encrypted table cannot be ruled out. Reporting
            // it as mentioned is the safe answer: it produces a warning about a statement that may not have
            // needed one, rather than silence about one that did.
            return true;
        }

        // Only word tokens are considered, which is what keeps a table's name inside a string literal or a
        // comment from counting as a mention. The word carries its bare text, with any quoting reported
        // separately, so a delimited name needs no unquoting here.
        foreach (Word word in tokens.OfType<Word>())
        {
            if (await this.metadataManager
                    .HasEncryptedColumnsAsync(word.Value, cancellationToken)
                    .ConfigureAwait(false))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Splits the text into tokens, or returns <see langword="null"/> when it cannot be tokenized at all.
    /// </summary>
    /// <remarks>
    /// Kept separate from its caller because the tokenizer is a <c>ref struct</c>, which cannot be held in a
    /// local of an asynchronous method.
    /// </remarks>
    private static IList<Token>? TryTokenize(string commandText, bool mySql)
    {
        try
        {
            Tokenizer tokenizer = default;
            return tokenizer.Tokenize(commandText, AdoDialect.For(mySql));
        }
        catch (TokenizeException)
        {
            return null;
        }
    }
}
