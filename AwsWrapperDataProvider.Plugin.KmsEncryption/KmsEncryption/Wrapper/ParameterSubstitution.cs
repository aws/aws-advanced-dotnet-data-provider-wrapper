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
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Wrapper;

/// <summary>
/// Swaps a command's parameter collection for one in which values bound to encrypted columns have been
/// replaced by ciphertext, and puts the original collection back when disposed.
/// </summary>
/// <remarks>
/// <para>
/// <b>The application's own parameter objects are never written to.</b> Encrypted values are carried by
/// freshly created substitutes; unencrypted parameters are re-added by reference. This is deliberate,
/// and each of the alternatives fails in a way no amount of care would fix:
/// </para>
/// <list type="bullet">
/// <item><description>
/// Assigning ciphertext to the application's parameter would be re-encrypted if the command ran a second
/// time - and running a command twice is ordinary: the failover plugin does not retry for the
/// application, it reports the failover and expects the call to be issued again. The result decrypts
/// cleanly into garbage rather than failing.
/// </description></item>
/// <item><description>
/// A strongly typed parameter rejects the assignment outright (assigning bytes to a parameter declared
/// for a string throws).
/// </description></item>
/// <item><description>
/// The application's <see cref="DbParameter.Size"/> survives an in-place change, so ciphertext is
/// silently truncated to the length of the original plaintext and the stored value is destroyed.
/// </description></item>
/// <item><description>
/// Restoring afterwards is lossy: a provider-specific type such as PostgreSQL <c>varchar</c> cannot be
/// recovered through the <see cref="DbParameter"/> surface and comes back as something else.
/// </description></item>
/// </list>
/// <para>
/// Substitutes are created with <see cref="DbCommand.CreateParameter"/> so they are the target driver's
/// own type, and <see cref="DbParameter.Size"/> is deliberately left at its default so nothing truncates.
/// </para>
/// </remarks>
internal sealed class ParameterSubstitution : IDisposable
{
    private readonly DbParameterCollection collection;
    private readonly DbParameter[] originals;
    private bool restored;

    private ParameterSubstitution(DbParameterCollection collection, DbParameter[] originals)
    {
        this.collection = collection;
        this.originals = originals;
    }

    /// <summary>
    /// Normalises a parameter name for comparison. Drivers differ over whether the collection keeps the
    /// marker that appears in the SQL, so the marker is dropped and case is ignored.
    /// </summary>
    internal static string NormaliseName(string? parameterName)
    {
        string name = (parameterName ?? string.Empty).Trim();
        if (name.Length > 0 && (name[0] == '@' || name[0] == ':' || name[0] == '?'))
        {
            name = name[1..];
        }

        return name.ToLowerInvariant();
    }

    /// <summary>
    /// Replaces the values bound to encrypted columns with ciphertext.
    /// </summary>
    /// <returns>
    /// A token that restores the original parameters when disposed. Dispose it in a <c>finally</c>, so the
    /// collection is restored even when execution throws.
    /// </returns>
    internal static Task<ParameterSubstitution> ApplyAsync(
        DbCommand command,
        IReadOnlyDictionary<string, ColumnEncryptionConfig> parametersToEncrypt,
        IColumnEncryptor encryptor,
        CancellationToken cancellationToken) =>
        ApplyCoreAsync(
            command.Parameters, command.CreateParameter, parametersToEncrypt, encryptor, cancellationToken);

    /// <summary>
    /// Replaces the values bound to encrypted columns with ciphertext, for one command of a batch.
    /// </summary>
    /// <remarks>
    /// A batch command is not a <see cref="DbCommand"/>, but it carries its own command text and its own
    /// parameter collection, so each one is substituted independently and on the same terms. Check
    /// <see cref="DbBatchCommand.CanCreateParameter"/> before calling: without it there is no way to make a
    /// substitute of the driver's own type, and writing to the application's parameter is not an option.
    /// </remarks>
    internal static Task<ParameterSubstitution> ApplyAsync(
        DbBatchCommand batchCommand,
        IReadOnlyDictionary<string, ColumnEncryptionConfig> parametersToEncrypt,
        IColumnEncryptor encryptor,
        CancellationToken cancellationToken) =>
        ApplyCoreAsync(
            batchCommand.Parameters,
            batchCommand.CreateParameter,
            parametersToEncrypt,
            encryptor,
            cancellationToken);

    /// <summary>
    /// Substitutes into a parameter collection. Shared by the command and batch-command paths, which differ
    /// only in where the collection and the factory for new parameters come from.
    /// </summary>
    private static async Task<ParameterSubstitution> ApplyCoreAsync(
        DbParameterCollection parameters,
        Func<DbParameter> createParameter,
        IReadOnlyDictionary<string, ColumnEncryptionConfig> parametersToEncrypt,
        IColumnEncryptor encryptor,
        CancellationToken cancellationToken)
    {
        DbParameter[] originals = parameters.Cast<DbParameter>().ToArray();

        // Encrypt before touching the collection: if any value fails, the command is left exactly as the
        // application built it.
        var ciphertexts = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (DbParameter original in originals)
        {
            string name = NormaliseName(original.ParameterName);
            if (!parametersToEncrypt.TryGetValue(name, out ColumnEncryptionConfig? column))
            {
                continue;
            }

            // A SQL NULL must stay a SQL NULL. Encrypting it would store ciphertext of "nothing", which
            // no longer reads back as NULL and defeats IS NULL predicates.
            if (original.Value is null || original.Value is DBNull)
            {
                continue;
            }

            ciphertexts[name] = await encryptor
                .EncryptAsync(original.Value, column, cancellationToken)
                .ConfigureAwait(false);
        }

        var substitution = new ParameterSubstitution(parameters, originals);
        if (ciphertexts.Count == 0)
        {
            return substitution;
        }

        parameters.Clear();
        foreach (DbParameter original in originals)
        {
            string name = NormaliseName(original.ParameterName);
            if (!ciphertexts.TryGetValue(name, out byte[]? ciphertext))
            {
                parameters.Add(original);
                continue;
            }

            DbParameter substitute = createParameter();
            substitute.ParameterName = original.ParameterName;
            substitute.Direction = original.Direction;
            substitute.Value = ciphertext;

            // Both MySQL drivers keep the original type when only Value changes, and would bind the
            // ciphertext as text. Size, Precision and Scale are left alone on purpose: copying the
            // application's Size truncates the ciphertext.
            substitute.DbType = DbType.Binary;

            parameters.Add(substitute);
        }

        return substitution;
    }

    /// <summary>Puts the application's original parameters back, in their original order.</summary>
    public void Dispose()
    {
        if (this.restored)
        {
            return;
        }

        this.restored = true;

        // Restoring must not mask the exception that is already in flight, so nothing here is allowed to
        // throw. Object identity and order are both preserved: the application may hold references to
        // these parameters and may look them up by name.
        try
        {
            this.collection.Clear();
            foreach (DbParameter original in this.originals)
            {
                this.collection.Add(original);
            }
        }
        catch (Exception)
        {
            // Intentionally swallowed. The command is already in an unusable state and the caller needs
            // to see the original failure, not a secondary one from cleanup.
        }
    }
}
