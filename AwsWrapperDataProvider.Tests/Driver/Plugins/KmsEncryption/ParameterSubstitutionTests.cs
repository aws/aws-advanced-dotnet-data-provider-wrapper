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
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Wrapper;
using Npgsql;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.KmsEncryption;

/// <summary>
/// Exercises the substitution against a real <see cref="NpgsqlCommand"/>. A hand-written fake command
/// would not reproduce the behaviours that forced this design - typed parameters rejecting a byte[],
/// Size surviving an in-place change, and provider-specific types being unrecoverable - so a real driver
/// object is used. No connection is opened; only the parameter collection is exercised.
/// </summary>
public class ParameterSubstitutionTests
{
    private static readonly ColumnEncryptionConfig SsnColumn = new("users", "ssn", "key-1", "AES-256-GCM");

    /// <summary>Returns a recognisable stand-in for ciphertext, longer than any plaintext used here.</summary>
    private sealed class FakeEncryptor : IColumnEncryptor
    {
        internal int Calls { get; private set; }

        public Task<byte[]> EncryptAsync(object value, ColumnEncryptionConfig column, CancellationToken cancellationToken)
        {
            this.Calls++;
            return Task.FromResult(Encoding.UTF8.GetBytes("CIPHERTEXT-OF-" + value));
        }
    }

    private sealed class ThrowingEncryptor : IColumnEncryptor
    {
        public Task<byte[]> EncryptAsync(object value, ColumnEncryptionConfig column, CancellationToken cancellationToken)
            => Task.FromException<byte[]>(new InvalidOperationException("kms unavailable"));
    }

    private static Dictionary<string, ColumnEncryptionConfig> EncryptSsn() =>
        new(StringComparer.Ordinal) { { "ssn", SsnColumn } };

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEncryptedParameterIsReplacedAndOriginalIsUntouched()
    {
        using var command = new NpgsqlCommand("INSERT INTO users (ssn) VALUES (@ssn)");
        NpgsqlParameter original = command.Parameters.AddWithValue("@ssn", "123-45-6789");
        original.Size = 11;
        original.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar;

        var encryptor = new FakeEncryptor();
        using (await ParameterSubstitution.ApplyAsync(
            command, EncryptSsn(), encryptor, TestContext.Current.CancellationToken))
        {
            DbParameter substitute = command.Parameters[0];
            Assert.NotSame(original, substitute);
            Assert.Equal("CIPHERTEXT-OF-123-45-6789", Encoding.UTF8.GetString((byte[])substitute.Value!));
            Assert.Equal(DbType.Binary, substitute.DbType);

            // Size must not be copied: the application's 11 would truncate the ciphertext and destroy
            // the stored value.
            Assert.Equal(0, substitute.Size);
        }

        // The application's parameter object is byte-for-byte as it was, including the provider-specific
        // type, which a mutate-and-restore approach cannot preserve.
        Assert.Same(original, command.Parameters[0]);
        Assert.Equal("123-45-6789", original.Value);
        Assert.Equal(11, original.Size);
        Assert.Equal(NpgsqlTypes.NpgsqlDbType.Varchar, original.NpgsqlDbType);
        Assert.Equal(1, encryptor.Calls);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestReExecutionDoesNotDoubleEncrypt()
    {
        using var command = new NpgsqlCommand("INSERT INTO users (ssn) VALUES (@ssn)");
        command.Parameters.AddWithValue("@ssn", "123-45-6789");
        var encryptor = new FakeEncryptor();

        // Running the same command twice is ordinary: the failover plugin reports a failover and expects
        // the call to be issued again. If the first run had written ciphertext into the application's
        // parameter, the second would encrypt the ciphertext and store irreversible garbage.
        for (int run = 0; run < 2; run++)
        {
            using (await ParameterSubstitution.ApplyAsync(
                command, EncryptSsn(), encryptor, TestContext.Current.CancellationToken))
            {
                Assert.Equal(
                    "CIPHERTEXT-OF-123-45-6789",
                    Encoding.UTF8.GetString((byte[])command.Parameters[0].Value!));
            }
        }

        Assert.Equal("123-45-6789", command.Parameters[0].Value);
        Assert.Equal(2, encryptor.Calls);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestTypedParameterIsSupported()
    {
        using var command = new NpgsqlCommand("INSERT INTO users (ssn) VALUES (@ssn)");

        // A strongly typed parameter cannot be mutated in place at all - assigning a byte[] to it throws -
        // so substitution is the only mechanism that works here.
        var typed = new NpgsqlParameter<string>("@ssn", "123-45-6789");
        command.Parameters.Add(typed);
        Assert.Throws<InvalidCastException>(() => typed.Value = new byte[] { 1, 2, 3 });

        using (await ParameterSubstitution.ApplyAsync(
            command, EncryptSsn(), new FakeEncryptor(), TestContext.Current.CancellationToken))
        {
            Assert.IsType<byte[]>(command.Parameters[0].Value);
        }

        Assert.Same(typed, command.Parameters[0]);
        Assert.Equal("123-45-6789", typed.Value);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("@ssn")]
    [InlineData("ssn")]
    [InlineData(":ssn")]
    [InlineData("SSN")]
    public async Task TestParameterNameMatchingIgnoresMarkerAndCase(string parameterName)
    {
        using var command = new NpgsqlCommand("INSERT INTO users (ssn) VALUES (@ssn)");
        command.Parameters.Add(new NpgsqlParameter(parameterName, "123-45-6789"));

        using (await ParameterSubstitution.ApplyAsync(
            command, EncryptSsn(), new FakeEncryptor(), TestContext.Current.CancellationToken))
        {
            Assert.IsType<byte[]>(command.Parameters[0].Value);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestUnencryptedParametersArePassedThroughByReferenceAndKeepOrder()
    {
        using var command = new NpgsqlCommand("INSERT INTO users (id, ssn, city) VALUES (@id, @ssn, @city)");
        NpgsqlParameter id = command.Parameters.AddWithValue("@id", 7);
        NpgsqlParameter ssn = command.Parameters.AddWithValue("@ssn", "123-45-6789");
        NpgsqlParameter city = command.Parameters.AddWithValue("@city", "Vancouver");

        using (await ParameterSubstitution.ApplyAsync(
            command, EncryptSsn(), new FakeEncryptor(), TestContext.Current.CancellationToken))
        {
            Assert.Equal(3, command.Parameters.Count);
            Assert.Same(id, command.Parameters[0]);
            Assert.NotSame(ssn, command.Parameters[1]);
            Assert.Same(city, command.Parameters[2]);

            // Name lookup must keep working while substituted; drivers and ORMs rely on it.
            Assert.IsType<byte[]>(command.Parameters["@ssn"].Value);
        }

        Assert.Same(id, command.Parameters[0]);
        Assert.Same(ssn, command.Parameters[1]);
        Assert.Same(city, command.Parameters[2]);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(null)]
    [InlineData("dbnull")]
    public async Task TestNullStaysNull(string? kind)
    {
        using var command = new NpgsqlCommand("INSERT INTO users (ssn) VALUES (@ssn)");
        object nullValue = kind == "dbnull" ? DBNull.Value : (object?)null!;
        NpgsqlParameter original = command.Parameters.Add(new NpgsqlParameter("@ssn", NpgsqlTypes.NpgsqlDbType.Varchar));
        original.Value = nullValue;

        var encryptor = new FakeEncryptor();
        using (await ParameterSubstitution.ApplyAsync(
            command, EncryptSsn(), encryptor, TestContext.Current.CancellationToken))
        {
            // Encrypting a NULL would store ciphertext of nothing, which stops reading back as NULL and
            // silently defeats IS NULL predicates.
            Assert.Same(original, command.Parameters[0]);
        }

        Assert.Equal(0, encryptor.Calls);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestEncryptionFailureLeavesTheCommandExactlyAsBuilt()
    {
        using var command = new NpgsqlCommand("INSERT INTO users (ssn) VALUES (@ssn)");
        NpgsqlParameter original = command.Parameters.AddWithValue("@ssn", "123-45-6789");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ParameterSubstitution.ApplyAsync(
                command, EncryptSsn(), new ThrowingEncryptor(), TestContext.Current.CancellationToken));

        // Values are encrypted before the collection is touched, so a KMS failure cannot leave the
        // command half-substituted.
        Assert.Single(command.Parameters);
        Assert.Same(original, command.Parameters[0]);
        Assert.Equal("123-45-6789", original.Value);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestRestoreHappensEvenWhenExecutionThrows()
    {
        using var command = new NpgsqlCommand("INSERT INTO users (ssn) VALUES (@ssn)");
        NpgsqlParameter original = command.Parameters.AddWithValue("@ssn", "123-45-6789");

        await Assert.ThrowsAsync<TimeoutException>(async () =>
        {
            using (await ParameterSubstitution.ApplyAsync(
                command, EncryptSsn(), new FakeEncryptor(), TestContext.Current.CancellationToken))
            {
                throw new TimeoutException("statement timed out");
            }
        });

        Assert.Same(original, command.Parameters[0]);
        Assert.Equal("123-45-6789", original.Value);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestDisposeIsIdempotent()
    {
        using var command = new NpgsqlCommand("INSERT INTO users (ssn) VALUES (@ssn)");
        NpgsqlParameter original = command.Parameters.AddWithValue("@ssn", "123-45-6789");

        ParameterSubstitution substitution =
            await ParameterSubstitution.ApplyAsync(
                command, EncryptSsn(), new FakeEncryptor(), TestContext.Current.CancellationToken);
        substitution.Dispose();
        substitution.Dispose();

        Assert.Single(command.Parameters);
        Assert.Same(original, command.Parameters[0]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestNothingToEncryptLeavesTheCollectionAlone()
    {
        using var command = new NpgsqlCommand("INSERT INTO users (city) VALUES (@city)");
        NpgsqlParameter city = command.Parameters.AddWithValue("@city", "Vancouver");

        var encryptor = new FakeEncryptor();
        using (await ParameterSubstitution.ApplyAsync(
            command, EncryptSsn(), encryptor, TestContext.Current.CancellationToken))
        {
            Assert.Same(city, command.Parameters[0]);
        }

        Assert.Equal(0, encryptor.Calls);
    }
}
