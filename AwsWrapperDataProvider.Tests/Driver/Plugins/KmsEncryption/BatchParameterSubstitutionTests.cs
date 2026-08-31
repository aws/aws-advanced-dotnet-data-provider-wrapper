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

using System.Data;
using System.Data.Common;
using System.Text;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Wrapper;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.KmsEncryption;

/// <summary>
/// Exercises substitution across the commands of a batch, against real driver batch objects. No connection
/// is opened; only the per-command parameter collections are exercised.
/// </summary>
public class BatchParameterSubstitutionTests
{
    private static readonly ColumnEncryptionConfig SsnColumn = new("users", "ssn", "key-1", "AES-256-GCM");

    private static Dictionary<string, ColumnEncryptionConfig> EncryptSsn() =>
        new(StringComparer.Ordinal) { { "ssn", SsnColumn } };

    private sealed class FakeEncryptor : IColumnEncryptor
    {
        internal int Calls { get; private set; }

        public Task<byte[]> EncryptAsync(
            object value, ColumnEncryptionConfig column, CancellationToken cancellationToken)
        {
            this.Calls++;
            return Task.FromResult(Encoding.UTF8.GetBytes("CIPHERTEXT-OF-" + value));
        }
    }

    /// <summary>Fails only for a chosen value, so a batch can be made to fail part way through.</summary>
    private sealed class FailsOnEncryptor : IColumnEncryptor
    {
        private readonly string failFor;

        internal FailsOnEncryptor(string failFor)
        {
            this.failFor = failFor;
        }

        public Task<byte[]> EncryptAsync(
            object value, ColumnEncryptionConfig column, CancellationToken cancellationToken) =>
            Equals(value, this.failFor)
                ? Task.FromException<byte[]>(new InvalidOperationException("kms unavailable"))
                : Task.FromResult(Encoding.UTF8.GetBytes("CIPHERTEXT-OF-" + value));
    }

    /// <summary>
    /// Both drivers must allow a parameter to be created on a batch command; without it a substitute of the
    /// driver's own type cannot be made and the plugin has to decline.
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public void TestBothDriversCanCreateParametersOnABatchCommand()
    {
        Assert.True(new NpgsqlBatch().CreateBatchCommand().CanCreateParameter);
        Assert.True(new MySqlBatch().CreateBatchCommand().CanCreateParameter);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEachCommandIsSubstitutedIndependently()
    {
        var batch = new NpgsqlBatch();
        DbBatchCommand first = AddCommand(batch, "111-11-1111", 1);
        DbBatchCommand second = AddCommand(batch, "222-22-2222", 2);

        var encryptor = new FakeEncryptor();
        var applied = new List<ParameterSubstitution>();
        foreach (DbBatchCommand command in batch.BatchCommands)
        {
            applied.Add(await ParameterSubstitution.ApplyAsync(
                command, EncryptSsn(), encryptor, TestContext.Current.CancellationToken));
        }

        Assert.Equal(2, applied.Count);
        Assert.Equal(2, encryptor.Calls);

        // Each command carries its own row's ciphertext, so no value has leaked across commands.
        Assert.Equal("CIPHERTEXT-OF-111-11-1111", Ciphertext(first));
        Assert.Equal("CIPHERTEXT-OF-222-22-2222", Ciphertext(second));

        // The id parameter is not encrypted and is still the application's own object.
        Assert.Equal(1, Value(first, "@id"));
        Assert.Equal(2, Value(second, "@id"));

        RestoreAll(applied);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestRestoringPutsEveryCommandBack()
    {
        var batch = new NpgsqlBatch();
        DbBatchCommand first = AddCommand(batch, "111-11-1111", 1);
        DbBatchCommand second = AddCommand(batch, "222-22-2222", 2);

        var applied = new List<ParameterSubstitution>();
        foreach (DbBatchCommand command in batch.BatchCommands)
        {
            applied.Add(await ParameterSubstitution.ApplyAsync(
                command, EncryptSsn(), new FakeEncryptor(), TestContext.Current.CancellationToken));
        }

        RestoreAll(applied);

        Assert.Equal("111-11-1111", Value(first, "@ssn"));
        Assert.Equal("222-22-2222", Value(second, "@ssn"));
        Assert.Equal(2, first.Parameters.Count);
        Assert.Equal(2, second.Parameters.Count);
    }

    /// <summary>
    /// The case that a single command does not have to handle: a failure on a later command must not leave
    /// the earlier ones holding ciphertext.
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestFailurePartWayThroughRestoresTheCommandsAlreadySubstituted()
    {
        var batch = new NpgsqlBatch();
        DbBatchCommand first = AddCommand(batch, "111-11-1111", 1);
        DbBatchCommand second = AddCommand(batch, "222-22-2222", 2);

        var encryptor = new FailsOnEncryptor("222-22-2222");
        var applied = new List<ParameterSubstitution>();

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            try
            {
                foreach (DbBatchCommand command in batch.BatchCommands)
                {
                    applied.Add(await ParameterSubstitution.ApplyAsync(
                        command, EncryptSsn(), encryptor, TestContext.Current.CancellationToken));
                }
            }
            catch
            {
                RestoreAll(applied);
                throw;
            }
        });

        // The first command was substituted before the second failed, and must have been put back.
        Assert.Equal("111-11-1111", Value(first, "@ssn"));
        Assert.Equal("222-22-2222", Value(second, "@ssn"));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestSubstitutionWorksOnAMySqlBatchToo()
    {
        var batch = new MySqlBatch();
        var command = (MySqlBatchCommand)batch.CreateBatchCommand();
        command.CommandText = "INSERT INTO users (id, ssn) VALUES (@id, @ssn)";
        command.Parameters.Add(new MySqlParameter("@id", 1));
        command.Parameters.Add(new MySqlParameter("@ssn", "333-33-3333"));
        batch.BatchCommands.Add(command);

        using (await ParameterSubstitution.ApplyAsync(
            command, EncryptSsn(), new FakeEncryptor(), TestContext.Current.CancellationToken))
        {
            Assert.Equal("CIPHERTEXT-OF-333-33-3333", Ciphertext(command));
            Assert.Equal(DbType.Binary, Parameter(command, "@ssn").DbType);
        }

        Assert.Equal("333-33-3333", Value(command, "@ssn"));
    }

    /// <summary>
    /// Restoring twice must be harmless. The batch path restores in a <c>finally</c>, which can run after an
    /// earlier restore has already happened on a failure path, so the guard has to live on the substitution
    /// itself rather than on whatever is holding it.
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestRestoringTwiceIsHarmless()
    {
        var batch = new NpgsqlBatch();
        DbBatchCommand command = AddCommand(batch, "111-11-1111", 1);

        ParameterSubstitution substitution = await ParameterSubstitution.ApplyAsync(
            command, EncryptSsn(), new FakeEncryptor(), TestContext.Current.CancellationToken);

        substitution.Dispose();
        substitution.Dispose();

        Assert.Equal("111-11-1111", Value(command, "@ssn"));
        Assert.Equal(2, command.Parameters.Count);
    }

    /// <summary>Restores every command, the way the plugin's batch path does in its finally block.</summary>
    private static void RestoreAll(List<ParameterSubstitution> applied)
    {
        foreach (ParameterSubstitution substitution in applied)
        {
            substitution.Dispose();
        }
    }

    private static DbBatchCommand AddCommand(NpgsqlBatch batch, string ssn, int id)
    {
        NpgsqlBatchCommand command = batch.CreateBatchCommand();
        command.CommandText = "INSERT INTO users (id, ssn) VALUES (@id, @ssn)";
        command.Parameters.Add(new NpgsqlParameter("@id", id));
        command.Parameters.Add(new NpgsqlParameter("@ssn", ssn));
        batch.BatchCommands.Add(command);
        return command;
    }

    private static DbParameter Parameter(DbBatchCommand command, string name) =>
        command.Parameters.Cast<DbParameter>().Single(p => p.ParameterName == name);

    private static object? Value(DbBatchCommand command, string name) => Parameter(command, name).Value;

    private static string Ciphertext(DbBatchCommand command) =>
        Encoding.UTF8.GetString((byte[])Parameter(command, "@ssn").Value!);
}
