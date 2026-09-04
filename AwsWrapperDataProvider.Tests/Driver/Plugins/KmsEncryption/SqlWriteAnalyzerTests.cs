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

using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Parser;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.KmsEncryption;

public class SqlWriteAnalyzerTests
{
    private static QueryAnalysis Single(string sql)
    {
        List<QueryAnalysis> all = SqlWriteAnalyzer.Analyze(sql);
        return Assert.Single(all);
    }

    private static string Written(QueryAnalysis s) =>
        string.Join(" ", s.WrittenColumnsByParameter.OrderBy(p => p.Key).Select(p => $"{p.Key}={p.Value}"));

    [Fact]
    [Trait("Category", "Unit")]
    public void TestPlainInsert()
    {
        QueryAnalysis s = Single("INSERT INTO users (id, ssn) VALUES (@id, @ssn)");
        Assert.Equal("users", s.Table);
        Assert.Equal("id=id ssn=ssn", Written(s));
        Assert.Empty(s.UnreadableReasons);
    }

    [Theory]
    [Trait("Category", "Unit")]
    // Values that are not parameters must not shift the pairing. Matching by placeholder position instead
    // attributes @ssn to the first column and leaves the encrypted column readable.
    [InlineData("INSERT INTO users (created_at, ssn) VALUES (now(), @ssn)", "created_at")]
    [InlineData("INSERT INTO users (id, ssn) VALUES (DEFAULT, @ssn)", "id")]
    [InlineData("INSERT INTO users (id, ssn) VALUES (1, @ssn)", "id")]
    [InlineData("INSERT INTO users (id, ssn) VALUES (nextval('s'), @ssn)", "id")]
    public void TestNonParameterValueDoesNotShiftThePairing(string sql, string skippedColumn)
    {
        QueryAnalysis s = Single(sql);
        Assert.Equal("ssn=ssn", Written(s));
        Assert.Contains(skippedColumn, s.ColumnsWrittenWithoutAParameter);
        Assert.Empty(s.UnreadableReasons);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestMultiRowInsertPairsEveryRow()
    {
        QueryAnalysis s = Single("INSERT INTO users (ssn) VALUES (@a), (@b), (@c)");
        Assert.Equal("a=ssn b=ssn c=ssn", Written(s));
        Assert.Empty(s.UnreadableReasons);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestUpsertAssignmentsArePaired()
    {
        QueryAnalysis s = Single(
            "INSERT INTO users (id, ssn) VALUES (@id, @ssn) ON CONFLICT (id) DO UPDATE SET ssn = @newSsn");
        Assert.Equal("id=id newSsn=ssn ssn=ssn", Written(s));
        Assert.Empty(s.UnreadableReasons);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestMySqlUpsertAssignmentsArePaired()
    {
        QueryAnalysis s = Single(
            "INSERT INTO users (id, ssn) VALUES (@id, @ssn) ON DUPLICATE KEY UPDATE ssn = @newSsn");
        Assert.Equal("id=id newSsn=ssn ssn=ssn", Written(s));
        Assert.Empty(s.UnreadableReasons);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestQuotedIdentifiersAndReturningAreHandled()
    {
        // The shape Entity Framework Core generates for PostgreSQL.
        QueryAnalysis s = Single(
            "INSERT INTO \"users\" (\"ssn\", \"city\") VALUES (@p0, @p1) RETURNING \"id\"");
        Assert.Equal("users", s.Table);
        Assert.Equal("p0=ssn p1=city", Written(s));
        Assert.Empty(s.UnreadableReasons);
    }

    /// <summary>
    /// Backticks are MySQL's identifier quoting, so the statement has to be read with the MySQL grammar.
    /// Reading it as PostgreSQL is a syntax error, which is the correct answer for that engine.
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestBackquotedIdentifiersAreHandled()
    {
        QueryAnalysis s = Assert.Single(
            SqlWriteAnalyzer.Analyze("INSERT INTO `users` (`ssn`) VALUES (@p0)", mySql: true));
        Assert.Equal("users", s.Table);
        Assert.Equal("p0=ssn", Written(s));
        Assert.Empty(s.UnreadableReasons);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestBackquotedIdentifiersAreRefusedAsPostgreSql()
    {
        QueryAnalysis s = Single("INSERT INTO `users` (`ssn`) VALUES (@p0)");
        Assert.NotEmpty(s.UnreadableReasons);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestSchemaQualifiedTableUsesTheTableName()
    {
        QueryAnalysis s = Single("INSERT INTO app.users (ssn) VALUES (@p0)");
        Assert.Equal("users", s.Table);
        Assert.Equal("p0=ssn", Written(s));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestUpdateSetAndPredicateAreSeparated()
    {
        QueryAnalysis s = Single("UPDATE users SET ssn = @s WHERE id = @id");
        Assert.Equal("users", s.Table);
        Assert.Equal("s=ssn", Written(s));

        // @id is a predicate, not a stored value, so it must never be encrypted as one.
        Assert.Equal("id", s.PredicateColumnsByParameter["id"]);
        Assert.Empty(s.UnreadableReasons);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("UPDATE users SET updated_at = now(), ssn = @s WHERE id = @id")]
    [InlineData("UPDATE users SET ssn = @s, updated_at = now() WHERE id = @id")]
    public void TestUpdateWithANonParameterAssignmentStillPairsCorrectly(string sql)
    {
        QueryAnalysis s = Single(sql);
        Assert.Equal("s=ssn", Written(s));
        Assert.Contains("updated_at", s.ColumnsWrittenWithoutAParameter);
        Assert.Empty(s.UnreadableReasons);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestPredicateOnAPotentiallyEncryptedColumnIsReported()
    {
        // Reported, not refused, here: whether it matters depends on the metadata. The planner refuses it
        // when the column turns out to be encrypted, because a fresh nonce per value means the comparison
        // can never match.
        QueryAnalysis s = Single("UPDATE users SET city = @c WHERE ssn = @s");
        Assert.Equal("c=city", Written(s));
        Assert.Equal("ssn", s.PredicateColumnsByParameter["s"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestDeletePredicateIsReported()
    {
        QueryAnalysis s = Single("DELETE FROM users WHERE ssn = @s");
        Assert.Equal("users", s.Table);
        Assert.Empty(s.WrittenColumnsByParameter);
        Assert.Equal("ssn", s.PredicateColumnsByParameter["s"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestQualifiedPredicateColumnUsesTheColumnName()
    {
        QueryAnalysis s = Single("UPDATE users SET city = @c WHERE users.ssn = @s");
        Assert.Equal("ssn", s.PredicateColumnsByParameter["s"]);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("INSERT INTO users VALUES (@a, @b)", "does not list its columns")]
    [InlineData("INSERT INTO users (ssn) SELECT ssn FROM staging", "does not use a VALUES clause")]
    [InlineData("MERGE INTO users USING staging ON (1=1)", "MERGE")]
    // REPLACE does not parse as PostgreSQL, so it is refused for being unreadable. The reason names the
    // position rather than the statement, because the reason is logged - see
    // TestUnreadableReasonNeverQuotesTheStatement.
    [InlineData("REPLACE INTO users (ssn) VALUES (@s)", "")]
    public void TestUnsupportedShapesAreRefused(string sql, string expectedFragment)
    {
        QueryAnalysis s = Single(sql);
        Assert.NotEmpty(s.UnreadableReasons);
        if (expectedFragment.Length > 0)
        {
            Assert.Contains(expectedFragment, string.Join(" ", s.UnreadableReasons));
        }
    }

    /// <summary>
    /// A joined UPDATE is read normally, because the target of a SET assignment is always a column of the
    /// table being updated - SQL does not allow it to be qualified with another table's name.
    /// </summary>
    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("UPDATE users u SET ssn = @s FROM orders o WHERE o.id = u.id")]
    [InlineData("UPDATE users SET ssn = @s FROM orders WHERE orders.id = users.id")]
    [InlineData("UPDATE users u SET ssn = @s, city = @c FROM orders o WHERE o.id = u.id")]
    public void TestJoinedUpdateIsStillPaired(string sql)
    {
        QueryAnalysis s = Single(sql);
        Assert.Equal("users", s.Table);
        Assert.Empty(s.UnreadableReasons);
        Assert.Equal("ssn", s.WrittenColumnsByParameter["s"]);
    }

    /// <summary>
    /// With another table joined in, only a predicate column qualified with the target table or its alias is
    /// attributed to this statement, so a joined table's column of the same name is not mistaken for it.
    /// </summary>
    [Theory]
    [Trait("Category", "Unit")]
    // The joined table's ssn, not this one's - must not be reported.
    [InlineData("UPDATE users u SET city = @c FROM orders o WHERE o.ssn = @s", false)]
    // The target table's own ssn, reached through its alias - must be reported.
    [InlineData("UPDATE users u SET city = @c FROM orders o WHERE u.ssn = @s", true)]
    [InlineData("UPDATE users SET city = @c FROM orders WHERE users.ssn = @s", true)]
    public void TestJoinedUpdatePredicateIsAttributedByQualifier(string sql, bool expectedReported)
    {
        QueryAnalysis s = Single(sql);
        Assert.Empty(s.UnreadableReasons);
        Assert.Equal(expectedReported, s.PredicateColumnsByParameter.Values.Contains("ssn"));
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("WITH x AS (SELECT 1) INSERT INTO users (id, ssn) VALUES (@id, @ssn)")]
    [InlineData("WITH RECURSIVE x AS (SELECT 1) INSERT INTO users (id, ssn) VALUES (@id, @ssn)")]
    [InlineData("WITH x AS (SELECT 1), y AS (SELECT 2) INSERT INTO users (id, ssn) VALUES (@id, @ssn)")]
    [InlineData("WITH x (a) AS (SELECT 1) INSERT INTO users (id, ssn) VALUES (@id, @ssn)")]
    [InlineData("WITH x AS MATERIALIZED (SELECT 1) INSERT INTO users (id, ssn) VALUES (@id, @ssn)")]
    [InlineData("WITH x AS NOT MATERIALIZED (SELECT 1) INSERT INTO users (id, ssn) VALUES (@id, @ssn)")]
    // A nested parenthesis in the body must not end the clause early.
    [InlineData("WITH x AS (SELECT (1 + 2)) INSERT INTO users (id, ssn) VALUES (@id, @ssn)")]
    public void TestCtePrefixedInsertIsAnalyzed(string sql)
    {
        QueryAnalysis s = Single(sql);
        Assert.Equal("users", s.Table);
        Assert.Empty(s.UnreadableReasons);
        Assert.Equal("id=id ssn=ssn", Written(s));
    }

    /// <summary>
    /// A CTE-prefixed UPDATE is valid PostgreSQL that the parser does not accept, so it is refused. The
    /// statement is never misread - it simply cannot be encrypted, and the caller is told so.
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestCtePrefixedUpdateIsRefused()
    {
        QueryAnalysis s = Single("WITH x AS (SELECT 1) UPDATE users SET ssn = @s WHERE id = @id");
        Assert.NotEmpty(s.UnreadableReasons);
    }

    /// <summary>
    /// A write inside the WITH clause is refused rather than stepped over, because those writes are not
    /// modelled and passing over the clause would let one through unnoticed. An INSERT there parses, so the
    /// reason names the clause; an UPDATE or DELETE there does not parse, so the reason is that the statement
    /// could not be read. Either way it is refused, which is what matters.
    /// </summary>
    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("WITH x AS (INSERT INTO users (ssn) VALUES (@s) RETURNING id) SELECT * FROM x", true)]
    [InlineData("WITH x AS (UPDATE users SET ssn = @s RETURNING id) SELECT * FROM x", false)]
    [InlineData("WITH x AS (DELETE FROM users RETURNING id) SELECT * FROM x", false)]
    public void TestWriteInsideACteIsRefused(string sql, bool namesTheClause)
    {
        QueryAnalysis s = Single(sql);
        Assert.NotEmpty(s.UnreadableReasons);
        if (namesTheClause)
        {
            Assert.Contains("WITH clause", string.Join(" ", s.UnreadableReasons));
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestFunctionWrappedParameterIsNotTreatedAsAStoredValue()
    {
        // The server would apply upper() to the ciphertext, so this value cannot be encrypted. It is
        // reported as a column written without a parameter, which the planner turns into a refusal if the
        // column is encrypted - the analyzer alone cannot know that.
        QueryAnalysis s = Single("INSERT INTO users (id, ssn) VALUES (@id, upper(@ssn))");
        Assert.DoesNotContain("ssn", s.WrittenColumnsByParameter.Values);
        Assert.Contains("ssn", s.ColumnsWrittenWithoutAParameter);
    }

    /// <summary>
    /// An unnamed placeholder cannot be matched to an entry in the parameter collection, whose order is not
    /// the order the placeholders appear in the text, so it is named as the reason.
    /// </summary>
    [Theory]
    [Trait("Category", "Unit")]
    // "$1" is PostgreSQL's own placeholder syntax and parses, so it is recognised and named as such.
    [InlineData("INSERT INTO users (ssn) VALUES ($1)", false)]
    // "?" is what MySQL uses, and MySqlConnector accepts it.
    [InlineData("INSERT INTO users (ssn) VALUES (?)", true)]
    [InlineData("UPDATE users SET ssn = ? WHERE id = ?", true)]
    public void TestUnnamedPlaceholdersAreRefused(string sql, bool mySql)
    {
        QueryAnalysis s = Assert.Single(SqlWriteAnalyzer.Analyze(sql, mySql));
        Assert.Contains("named parameters", string.Join(" ", s.UnreadableReasons));
    }

    /// <summary>
    /// PostgreSQL has no "?" placeholder, so reading one as PostgreSQL is a syntax error. The statement is
    /// still refused - which is what matters - but the reason is that it could not be read, rather than
    /// naming the placeholder.
    /// </summary>
    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("INSERT INTO users (ssn) VALUES (?)")]
    [InlineData("UPDATE users SET ssn = ? WHERE id = ?")]
    public void TestQuestionMarkPlaceholderIsRefusedAsPostgreSql(string sql)
    {
        QueryAnalysis s = Single(sql);
        Assert.NotEmpty(s.UnreadableReasons);
    }

    /// <summary>
    /// The reason a statement could not be read must never quote the statement.
    /// </summary>
    /// <remarks>
    /// The parser names the token it did not expect, and that token can be a string literal. Since this
    /// reason is logged as a warning, echoing it would write a readable value - exactly the value the caller
    /// was trying to store in an encrypted column - into the application log. Each statement below is
    /// malformed in a way that makes the sensitive literal the unexpected token.
    /// </remarks>
    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("INSERT INTO users (ssn) VALUES ('111-22-3333' '444-55-6666')", "444-55-6666")]
    [InlineData("UPDATE users SET ssn 'my-secret-value' WHERE id = 1", "my-secret-value")]
    [InlineData("SELECT * FROM users WHERE ssn 'patient-record-42'", "patient-record-42")]
    [InlineData(@"INSERT INTO users (ssn) VALUES (E'123-45-6789\';x')", "123-45-6789")]
    public void TestUnreadableReasonNeverQuotesTheStatement(string sql, string sensitive)
    {
        QueryAnalysis s = Single(sql);
        string reason = string.Join(" ", s.UnreadableReasons);

        Assert.NotEmpty(s.UnreadableReasons);
        Assert.DoesNotContain(sensitive, reason, StringComparison.Ordinal);

        // The position is kept, because it is what makes the warning actionable.
        Assert.Contains("line", reason, StringComparison.Ordinal);
        Assert.Contains("column", reason, StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestMultiStatementCommandIsAnalyzedPerStatement()
    {
        // Entity Framework Core batches modifications into a single command.
        List<QueryAnalysis> all = SqlWriteAnalyzer.Analyze(
            "INSERT INTO users (ssn) VALUES (@p0);\nUPDATE users SET city = @p1 WHERE id = @p2;");

        Assert.Equal(2, all.Count);
        Assert.Equal("p0=ssn", Written(all[0]));
        Assert.Equal("p1=city", Written(all[1]));
        Assert.Equal("id", all[1].PredicateColumnsByParameter["p2"]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestSemicolonInsideALiteralDoesNotSplitTheStatement()
    {
        // Splitting on a bare semicolon would break this into two statements and mis-pair the columns.
        List<QueryAnalysis> all = SqlWriteAnalyzer.Analyze(
            "INSERT INTO users (note, ssn) VALUES ('a;b', @s)");

        Assert.Single(all);
        Assert.Equal("s=ssn", Written(all[0]));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestSemicolonInsideACommentDoesNotSplitTheStatement()
    {
        List<QueryAnalysis> all = SqlWriteAnalyzer.Analyze(
            "INSERT INTO users (ssn) /* one; two */ VALUES (@s) -- trailing; comment");

        Assert.Single(all);
        Assert.Equal("s=ssn", Written(all[0]));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestDollarQuotedStringDoesNotSplitTheStatement()
    {
        List<QueryAnalysis> all = SqlWriteAnalyzer.Analyze(
            "INSERT INTO users (note, ssn) VALUES ($tag$a;b$tag$, @s)");

        Assert.Single(all);
        Assert.Equal("s=ssn", Written(all[0]));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestEscapedQuoteInsideALiteralIsHandled()
    {
        List<QueryAnalysis> all = SqlWriteAnalyzer.Analyze(
            "INSERT INTO users (note, ssn) VALUES ('it''s; fine', @s)");

        Assert.Single(all);
        Assert.Equal("s=ssn", Written(all[0]));
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("SELECT ssn FROM users WHERE id = @id")]
    [InlineData("SET application_name = 'x'")]
    [InlineData("BEGIN")]
    [InlineData("CREATE TABLE t (a int)")]
    public void TestStatementsThatWriteNoColumnValuesAreIrrelevant(string sql)
    {
        QueryAnalysis s = Single(sql);
        Assert.Empty(s.UnreadableReasons);
        Assert.Empty(s.WrittenColumnsByParameter);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestColumnCountMismatchIsRefused()
    {
        QueryAnalysis s = Single("INSERT INTO users (id, ssn) VALUES (@id)");
        Assert.Contains("lists 2 columns", string.Join(" ", s.UnreadableReasons));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestEmptyAndWhitespaceCommandsProduceNothing()
    {
        Assert.Empty(SqlWriteAnalyzer.Analyze(string.Empty));
        Assert.Empty(SqlWriteAnalyzer.Analyze("   \n\t "));
        Assert.Empty(SqlWriteAnalyzer.Analyze(";;"));
    }
}
