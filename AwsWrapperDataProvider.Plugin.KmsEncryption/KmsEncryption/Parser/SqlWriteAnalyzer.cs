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
using AwsWrapperDataProvider.Plugin.KmsEncryption.Properties;
using SqlParser;
using SqlParser.Ast;
using SqlParser.Dialects;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Parser;

/// <summary>
/// Works out, for each statement in a command, which named parameter supplies which column's value.
/// </summary>
/// <remarks>
/// <para>
/// Statements are parsed into a syntax tree by SqlParserCS, and the tree is read for the three things this
/// plugin needs: the column each parameter is written to, the columns written by something that is not a
/// parameter, and the parameters compared against a column in a predicate.
/// </para>
/// <para>
/// A parameter is recognised by its leading marker in an identifier's text, because <c>@name</c> is an
/// ADO.NET convention rather than server syntax - see <see cref="AdoPostgreSqlDialect"/>. Anything this
/// analyzer cannot read is reported as a failure rather than guessed at; a wrong answer here stores readable
/// data in a column that is supposed to be encrypted, or encrypts under the wrong column's key, and neither
/// is visible at the time it happens.
/// </para>
/// </remarks>
internal static class SqlWriteAnalyzer
{
    private const string UnnamedPlaceholderReason =
        "unnamed placeholders cannot be matched to a column; use named parameters";

    private static readonly IReadOnlyDictionary<string, string> Empty =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    private static readonly string[] NoColumns = Array.Empty<string>();

    /// <summary>Analyzes every statement in a command.</summary>
    /// <param name="commandText">The command text, which may hold more than one statement.</param>
    /// <param name="mySql">
    /// Whether to read the text as MySQL rather than PostgreSQL. The two differ in ways that matter here -
    /// backtick versus double-quote identifiers, dollar-quoted strings, and the upsert clause - so the wrong
    /// choice turns readable SQL into a parse failure.
    /// </param>
    internal static List<QueryAnalysis> Analyze(string commandText, bool mySql = false)
    {
        // Nothing to read, and the parser treats an empty command as a syntax error rather than as no
        // statements, so it is answered here.
        if (string.IsNullOrWhiteSpace(commandText.Trim(' ', '\t', '\r', '\n', ';')))
        {
            return new List<QueryAnalysis>();
        }

        Dialect dialect = AdoDialect.For(mySql);

        Sequence<Statement> statements;
        try
        {
            statements = new SqlParser.Parser().ParseSql(commandText, dialect);
        }
        catch (ParserException ex)
        {
            // A command that fails to parse tells us nothing - including whether it writes to an encrypted
            // column - so it is reported as one unreadable statement, and the planner decides whether that
            // matters by looking for an encrypted table's name in the text.
            return new List<QueryAnalysis> { Unreadable(nameof(ParserException), ex.Line, ex.Column) };
        }
        catch (TokenizeException ex)
        {
            return new List<QueryAnalysis> { Unreadable(nameof(TokenizeException), ex.Line, ex.Column) };
        }

        var results = new List<QueryAnalysis>();
        foreach (Statement statement in statements)
        {
            results.Add(AnalyzeStatement(statement));
        }

        return results;
    }

    private static QueryAnalysis AnalyzeStatement(Statement statement)
    {
        switch (statement)
        {
            case Statement.Insert insert:
                return AnalyzeInsert(insert.InsertOperation);

            case Statement.Update update:
                return AnalyzeUpdate(update);

            case Statement.Delete delete:
                return AnalyzeDelete(delete.DeleteOperation);

            // A common table expression is modelled as a query whose body is the statement it prefixes, so a
            // CTE-prefixed write arrives here rather than as an Insert or Update. Unwrapping it is what keeps
            // "WITH x AS (...) INSERT ..." from looking like a read.
            case Statement.Select select:
                return AnalyzeQuery(select.Query);

            case Statement.Merge:
                return Unreadable("MERGE statements are not supported");

            default:
                // SELECT, DDL, SET, and anything else writes no column value through a parameter.
                return Irrelevant();
        }
    }

    private static QueryAnalysis AnalyzeQuery(Query query)
    {
        // A write inside the WITH clause - "WITH x AS (INSERT ... RETURNING id) SELECT ..." is valid
        // PostgreSQL - is not modelled, and passing over it would let a write to an encrypted column through
        // unnoticed.
        if (query.With is not null)
        {
            foreach (CommonTableExpression cte in query.With.CteTables)
            {
                if (ContainsWrite(cte.Query))
                {
                    return Unreadable(
                        "a WITH clause of this statement writes to a table, and a write inside a WITH clause "
                        + "is not supported");
                }
            }
        }

        return query.Body switch
        {
            SetExpression.Insert insert => AnalyzeStatement(insert.Statement),
            _ => Irrelevant(),
        };
    }

    // Only an INSERT is representable as a query body; a CTE containing an UPDATE or DELETE does not parse
    // at all, so the whole statement is already reported as unreadable before reaching here.
    private static bool ContainsWrite(Query query) => query.Body is SetExpression.Insert;

    private static QueryAnalysis AnalyzeInsert(InsertOperation insert)
    {
        string table = LastNamePart(insert.Name.ToString());

        // A column list is required: without it the column order would have to come from the database
        // catalogue, and guessing it would write values into the wrong columns.
        if (insert.Columns is null || insert.Columns.Count == 0)
        {
            return Unreadable(
                $"INSERT INTO {table} does not list its columns, so parameters cannot be matched to columns");
        }

        var columns = insert.Columns.Select(c => Unquote(c.ToString()!)).ToList();

        Values? values = FindValues(insert.Source);
        if (values is null)
        {
            // INSERT ... SELECT, INSERT ... DEFAULT VALUES, and similar. The values never pass through the
            // client, so there is nothing the plugin can encrypt.
            return Unreadable($"INSERT INTO {table} does not use a VALUES clause");
        }

        var writtenByParameter = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var writtenWithoutParameter = new List<string>();

        // Every row is paired against the same column list, so a multi-row insert is handled by repeating.
        foreach (Sequence<Expression> row in values.Rows)
        {
            if (row.Count != columns.Count)
            {
                return Unreadable(
                    $"INSERT INTO {table} lists {columns.Count} columns but a VALUES row supplies "
                    + $"{row.Count}, so they cannot be matched");
            }

            for (int c = 0; c < columns.Count; c++)
            {
                string? refusal = PairColumnWithValue(columns[c], row[c], writtenByParameter, writtenWithoutParameter);
                if (refusal is not null)
                {
                    return Unreadable(refusal);
                }
            }
        }

        if (values.Rows.Count == 0)
        {
            return Unreadable($"INSERT INTO {table} has no readable VALUES row");
        }

        // An upsert assigns to the same columns a second time, and those assignments are separate values that
        // also have to be encrypted.
        foreach (Statement.Assignment assignment in UpsertAssignments(insert.On))
        {
            string? assigned = TargetColumn(assignment.Target);
            if (assigned is null)
            {
                return Unreadable($"an assignment in the upsert clause of INSERT INTO {table} could not be read");
            }

            string? refusal = PairColumnWithValue(assigned, assignment.Value, writtenByParameter, writtenWithoutParameter);
            if (refusal is not null)
            {
                return Unreadable(refusal);
            }
        }

        return new QueryAnalysis(table, writtenByParameter, Empty, writtenWithoutParameter, NoColumns);
    }

    private static QueryAnalysis AnalyzeUpdate(Statement.Update update)
    {
        if (update.Table.Relation is not TableFactor.Table target)
        {
            return Unreadable("the target table of the UPDATE could not be identified");
        }

        string table = LastNamePart(target.Name.ToString());

        var writtenByParameter = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var writtenWithoutParameter = new List<string>();
        foreach (Statement.Assignment assignment in update.Assignments)
        {
            // A join does not make an assignment ambiguous: the target of a SET assignment is always a column
            // of the table being updated, because SQL does not allow it to be qualified with another table's
            // name.
            string? assigned = TargetColumn(assignment.Target);
            if (assigned is null)
            {
                return Unreadable($"an assignment in the SET clause of UPDATE {table} could not be read");
            }

            string? refusal = PairColumnWithValue(assigned, assignment.Value, writtenByParameter, writtenWithoutParameter);
            if (refusal is not null)
            {
                return Unreadable(refusal);
            }
        }

        // With another table joined in, "WHERE o.ssn = @s" names the joined table's column, not this one's.
        // Qualified names are therefore only attributed to this statement when the qualifier is the target
        // table or its alias.
        bool joined = update.From is not null || update.Table.Joins?.Count > 0;
        HashSet<string>? qualifiers = joined
            ? Qualifiers(table, target.Alias?.Name.ToString())
            : null;

        var predicatesByParameter = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        ReadPredicate(update.Selection, predicatesByParameter, qualifiers);

        return new QueryAnalysis(table, writtenByParameter, predicatesByParameter, writtenWithoutParameter, NoColumns);
    }

    private static QueryAnalysis AnalyzeDelete(DeleteOperation delete)
    {
        Sequence<TableWithJoins>? from = (delete.From as FromTable)?.From;
        if (from is null || from.Count == 0 || from[0].Relation is not TableFactor.Table target)
        {
            return Irrelevant();
        }

        string table = LastNamePart(target.Name.ToString());

        // A DELETE writes nothing, but it may compare against an encrypted column, which cannot work.
        var predicatesByParameter = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        ReadPredicate(delete.Selection, predicatesByParameter, null);

        return new QueryAnalysis(table, Empty, predicatesByParameter, NoColumns, NoColumns);
    }

    /// <summary>
    /// Files one column and the value written to it, under whichever of the two headings applies, and reports
    /// the reason the statement cannot be handled when neither does.
    /// </summary>
    /// <remarks>
    /// Only a value that is a parameter and nothing else can be encrypted. A literal, <c>DEFAULT</c>, or an
    /// expression such as <c>upper(@x)</c> is reported as a column written without a parameter, because the
    /// value the server stores would not be the value the plugin encrypted.
    /// </remarks>
    /// <param name="column">The column being written.</param>
    /// <param name="value">The value written to it.</param>
    /// <param name="writtenByParameter">
    /// Receives parameter-name to column, for each value that is a plain named parameter. These are the
    /// values the plugin can encrypt.
    /// </param>
    /// <param name="writtenWithoutParameter">
    /// Receives the column, for each value that is not a parameter - a literal, <c>DEFAULT</c>, or an
    /// expression such as <c>upper(@x)</c>. The plugin cannot encrypt these, because what the server stores
    /// is not what was bound, so the planner reports them if the column turns out to be encrypted.
    /// </param>
    /// <returns>
    /// <see langword="null"/> when the pair was filed, or the reason the whole statement must be refused.
    /// Returning the reason rather than a flag keeps it with the code that detected it, so a caller does not
    /// have to know which of several causes applied.
    /// </returns>
    private static string? PairColumnWithValue(
        string column,
        Expression value,
        Dictionary<string, string> writtenByParameter,
        List<string> writtenWithoutParameter)
    {
        if (ContainsUnnamedPlaceholder(value))
        {
            return UnnamedPlaceholderReason;
        }

        string? parameter = AsParameterName(value);
        if (parameter is null)
        {
            writtenWithoutParameter.Add(column);
            return null;
        }

        writtenByParameter[parameter] = column;
        return null;
    }

    /// <summary>
    /// Returns whether the value uses an unnamed placeholder, which cannot be matched to an entry in the
    /// parameter collection because that collection's order is not the order the placeholders appear in the
    /// text, and no driver exposes the placeholder order.
    /// </summary>
    private static bool ContainsUnnamedPlaceholder(Expression value) =>
        value is Expression.LiteralValue { Value: Value.Placeholder };

    /// <summary>
    /// Returns the parameter name when the expression is nothing but a named parameter placeholder.
    /// </summary>
    private static string? AsParameterName(Expression value)
    {
        // "@name" and ":name" arrive as an identifier whose text carries the marker, because the dialect is
        // told that the marker starts an identifier.
        if (value is Expression.Identifier identifier)
        {
            string text = identifier.Ident.ToString()!;
            return text.Length > 1 && (text[0] == '@' || text[0] == ':') ? text[1..] : null;
        }

        // "?" and "$1" are placeholders proper. Neither can be matched to an entry in the parameter
        // collection, whose order is not the order the placeholders appear in the text.
        return null;
    }

    /// <summary>
    /// Records parameters compared against a column, so that a comparison against an encrypted column can be
    /// reported.
    /// </summary>
    private static void ReadPredicate(
        Expression? expression,
        Dictionary<string, string> predicatesByParameter,
        HashSet<string>? onlyQualifiedBy)
    {
        switch (expression)
        {
            case null:
                return;

            case Expression.BinaryOp binary:
                if (!TryRecordComparison(binary, predicatesByParameter, onlyQualifiedBy))
                {
                    ReadPredicate(binary.Left, predicatesByParameter, onlyQualifiedBy);
                    ReadPredicate(binary.Right, predicatesByParameter, onlyQualifiedBy);
                }

                return;

            case Expression.Nested nested:
                ReadPredicate(nested.Expression, predicatesByParameter, onlyQualifiedBy);
                return;

            case Expression.UnaryOp unary:
                ReadPredicate(unary.Expression, predicatesByParameter, onlyQualifiedBy);
                return;

            default:
                return;
        }
    }

    private static bool TryRecordComparison(
        Expression.BinaryOp binary,
        Dictionary<string, string> predicatesByParameter,
        HashSet<string>? onlyQualifiedBy)
    {
        (string Column, string? Qualifier)? column =
            AsColumnReference(binary.Left) ?? AsColumnReference(binary.Right);
        string? parameter = AsParameterName(binary.Right) ?? AsParameterName(binary.Left);

        if (column is null || parameter is null)
        {
            return false;
        }

        if (onlyQualifiedBy is not null
            && (column.Value.Qualifier is null || !onlyQualifiedBy.Contains(column.Value.Qualifier)))
        {
            return true;
        }

        predicatesByParameter[parameter] = column.Value.Column;
        return true;
    }

    /// <summary>
    /// Returns the column and its qualifier when the expression is a column reference rather than a
    /// parameter.
    /// </summary>
    private static (string Column, string? Qualifier)? AsColumnReference(Expression expression)
    {
        switch (expression)
        {
            case Expression.Identifier identifier when AsParameterName(identifier) is null:
                return (Unquote(identifier.Ident.ToString()!), null);

            case Expression.CompoundIdentifier compound when compound.Idents.Count >= 2:
                return (
                    Unquote(compound.Idents[^1].ToString()!),
                    Unquote(compound.Idents[^2].ToString()!));

            default:
                return null;
        }
    }

    /// <summary>Finds the VALUES rows of an INSERT, if it has any.</summary>
    private static Values? FindValues(Statement.Select? source) =>
        source?.Query.Body is SetExpression.ValuesExpression values ? values.Values : null;

    private static IEnumerable<Statement.Assignment> UpsertAssignments(OnInsert? on) =>
        on switch
        {
            OnInsert.Conflict { OnConflict.OnConflictAction: OnConflictAction.DoUpdate update } =>
                update.DoUpdateAction.Assignments,
            OnInsert.DuplicateKeyUpdate duplicate => duplicate.Assignments,
            _ => Array.Empty<Statement.Assignment>(),
        };

    private static HashSet<string> Qualifiers(string table, string? alias)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { table };
        if (!string.IsNullOrEmpty(alias))
        {
            names.Add(Unquote(alias));
        }

        return names;
    }

    /// <summary>
    /// Returns the column an assignment writes to, or <see langword="null"/> when the target is not a plain
    /// column - a tuple assignment such as <c>SET (a, b) = (...)</c> pairs several columns with one value
    /// list, which this analyzer does not model.
    /// </summary>
    /// <remarks>
    /// The target has to be read from the node rather than rendered, because an assignment target is one of
    /// the few nodes in this parser that does not print itself as SQL.
    /// </remarks>
    private static string? TargetColumn(AssignmentTarget target) =>
        target is AssignmentTarget.ColumnName column ? LastNamePart(column.Name.ToString()) : null;

    /// <summary>Returns the last part of a possibly schema-qualified name, unquoted.</summary>
    private static string LastNamePart(string name)
    {
        int dot = name.LastIndexOf('.');
        return Unquote(dot >= 0 ? name[(dot + 1)..] : name);
    }

    /// <summary>
    /// Removes the quoting the parser preserves around a delimited identifier.
    /// </summary>
    /// <remarks>
    /// The parser reports an identifier as it was written, so a MySQL <c>`ssn`</c> and a PostgreSQL
    /// <c>"ssn"</c> both keep their delimiters. The encryption metadata holds bare names, so they have to
    /// come off before a comparison.
    /// </remarks>
    private static string Unquote(string name) => name.Trim('`', '"', '[', ']', ' ');

    private static QueryAnalysis Irrelevant() =>
        new(null, Empty, Empty, NoColumns, NoColumns);

    /// <summary>
    /// Reports that a statement could not be parsed, naming only where parsing stopped.
    /// </summary>
    /// <remarks>
    /// The parser's own message is deliberately discarded. It names the token it did not expect, and that
    /// token can be a string literal taken from the statement - so a malformed write of a readable value into
    /// an encrypted column would put that value into the log, which is the one thing this plugin must never
    /// do. The position and the kind of failure are equally actionable and cannot carry any content.
    /// </remarks>
    private static QueryAnalysis Unreadable(string failureKind, long line, long column) =>
        Unreadable(string.Format(
            CultureInfo.CurrentCulture,
            Resources.SqlWriteAnalyzer_Analyze_Unreadable,
            failureKind,
            line,
            column));

    private static QueryAnalysis Unreadable(string reason) =>
        new(null, Empty, Empty, NoColumns, new[] { reason });
}
