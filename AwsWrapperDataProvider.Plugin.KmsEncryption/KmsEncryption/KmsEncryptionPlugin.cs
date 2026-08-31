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

using System.Data.Common;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Key;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Metadata;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Wrapper;
using AwsWrapperDataProvider.Plugin.KmsEncryption.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption;

/// <summary>
/// Provides transparent client-side encryption of configured columns using AWS Key Management Service
/// (KMS).
/// <para>
/// Values bound to an encrypted column are encrypted before they reach the database, and values read
/// back are decrypted before the application sees them, so no application code changes are required.
/// Which columns are encrypted, and with which key and algorithm, comes from the encryption metadata in
/// the schema named by <see cref="PropertyDefinition.KmsEncryptionMetadataSchema"/>.
/// </para>
/// <para>
/// Encryption uses envelope encryption: a KMS master key protects per-column data keys, and the data
/// keys perform the encryption locally. Data keys are cached in memory so the common path does not call
/// KMS.
/// </para>
/// <para>
/// <b>A statement this plugin cannot encrypt is logged as a warning and then executed unchanged.</b> It
/// is not refused. The consequence is that readable data can reach a column the application believes is
/// encrypted, so a server-side constraint is the backstop rather than the driver - see the "Rejecting
/// plaintext at the server" section of the plugin documentation, which every deployment should follow.
/// The cases involved are listed on <see cref="WarnIfUnsupported"/>.
/// </para>
/// </summary>
public class KmsEncryptionPlugin : AbstractConnectionPlugin
{
    private static readonly ILogger<KmsEncryptionPlugin> Logger =
        LoggerUtils.GetLogger<KmsEncryptionPlugin>();

    private readonly IPluginService pluginService;
    private readonly Dictionary<string, string> props;
    private IStatementEncryptionPlanner? planner;
    private IColumnEncryptor? encryptor;
    private MetadataManager? metadataManager;
    private KeyManager? keyManager;
    private IDisposable[] owned = Array.Empty<IDisposable>();

    internal KmsEncryptionPlugin(IPluginService pluginService, Dictionary<string, string> props)
    {
        this.pluginService = pluginService;
        this.props = props;
    }

    internal KmsEncryptionPlugin(
        IPluginService pluginService,
        Dictionary<string, string> props,
        IStatementEncryptionPlanner planner,
        IColumnEncryptor encryptor)
        : this(pluginService, props)
    {
        this.planner = planner;
        this.encryptor = encryptor;
    }

    /// <summary>
    /// Supplies the collaborators after construction.
    /// </summary>
    /// <remarks>
    /// The metadata manager has to be able to open a connection with this plugin skipped, so it needs the
    /// plugin instance; the plugin needs the manager to plan statements. Joining them here breaks that cycle
    /// without either of them having to be mutable afterwards.
    /// </remarks>
    internal void Initialise(
        IStatementEncryptionPlanner statementPlanner,
        IColumnEncryptor columnEncryptor,
        MetadataManager metadata,
        KeyManager keys)
    {
        this.planner = statementPlanner;
        this.encryptor = columnEncryptor;
        this.metadataManager = metadata;
        this.keyManager = keys;
        this.owned = new IDisposable[] { metadata, keys };
    }

    /// <summary>
    /// Discards the cached encryption metadata, so the next statement reads it from the database again.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Registering a column for encryption is something an operator does while applications are running.
    /// Without this the change is only noticed once the cached copy expires - up to
    /// <see cref="PropertyDefinition.KmsMetadataCacheExpirationMinutes"/> later - so this is the way to make
    /// a schema change take effect immediately.
    /// </para>
    /// <para>
    /// It affects every connection in this process, and it discards rather than reloads: nothing is read
    /// until the next statement needs it, so this cannot fail.
    /// </para>
    /// </remarks>
    public static void ClearCache() => MetadataManager.ClearCache();

    /// <summary>
    /// Gets the methods this plugin participates in. Encryption applies to parameter binding and result
    /// reads, so the plugin subscribes to the command, batch, and reader pipelines.
    /// </summary>
    public override IReadOnlySet<string> SubscribedMethods { get; } = new HashSet<string>(
        PluginMethods.CommandMethods
            .Concat(PluginMethods.BatchMethods)
            .Concat(PluginMethods.ReaderMethods));

    public override async Task<T> Execute<T>(
        object methodInvokedOn,
        string methodName,
        ADONetDelegate<T> methodFunc,
        params object[] methodArgs)
    {
        // A batch is not a command, and each of its commands carries its own command text and its own
        // parameter collection, so the command path below cannot see them. Each is planned and substituted
        // separately instead.
        if (methodInvokedOn is DbBatch batch)
        {
            return await this.ExecuteBatchAsync(batch, methodName, methodFunc).ConfigureAwait(false);
        }

        if (methodInvokedOn is not DbCommand command || string.IsNullOrEmpty(command.CommandText))
        {
            return await methodFunc().ConfigureAwait(false);
        }

        StatementEncryptionPlan plan = await this.Planner
            .PlanAsync(command.CommandText, CancellationToken.None)
            .ConfigureAwait(false);

        if (plan.IsNoOp)
        {
            // No parameter needs encrypting, but the results may still contain encrypted columns - a plain
            // SELECT is the common case - so the reader is still offered for decryption.
            T untouched = await methodFunc().ConfigureAwait(false);
            return await this.DecryptResultIfNeededAsync(untouched).ConfigureAwait(false);
        }

        this.WarnIfUnsupported(plan, methodName, command.Parameters);

        using (await ParameterSubstitution
            .ApplyAsync(command, plan.ParametersToEncrypt, this.Encryptor, CancellationToken.None)
            .ConfigureAwait(false))
        {
            T result = await methodFunc().ConfigureAwait(false);
            return await this.DecryptResultIfNeededAsync(result).ConfigureAwait(false);
        }
    }

    private IStatementEncryptionPlanner Planner =>
        this.planner ?? throw new InvalidOperationException(
            Resources.KmsEncryptionPlugin_Initialise_NotInitialised);

    private IColumnEncryptor Encryptor =>
        this.encryptor ?? throw new InvalidOperationException(
            Resources.KmsEncryptionPlugin_Initialise_NotInitialised);

    /// <summary>
    /// Wraps a returned reader so that encrypted columns are decrypted as the application reads them.
    /// </summary>
    /// <remarks>
    /// Which columns are encrypted is taken from the result metadata rather than from the query, so joins,
    /// views, and subqueries all resolve correctly. Anything that is not a reader is returned untouched,
    /// because only a reader carries the column information decryption needs - a scalar result is checked by
    /// <see cref="WarnIfScalarIsCiphertext"/> instead.
    /// </remarks>
    private async Task<T> DecryptResultIfNeededAsync<T>(T result)
    {
        if (result is not DbDataReader reader || this.metadataManager is null || this.keyManager is null)
        {
            WarnIfScalarIsCiphertext(result);
            return result;
        }

        DbDataReader wrapped = await DecryptingDataReader.CreateAsync(
            reader,
            this.metadataManager,
            new EncryptionService(),
            (column, token) => this.keyManager.GetDataKeyAsync(column.DataKeyMetadata!, token),
            CancellationToken.None).ConfigureAwait(false);

        return (T)(object)wrapped;
    }

    /// <summary>
    /// Warns when a result that is not a reader carries what look like encrypted bytes.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>ExecuteScalar</c> hands the value straight to the application, and there is no reader to wrap, so
    /// an encrypted column arrives as its stored bytes. Nor can it be decrypted here: a scalar carries no
    /// column metadata, so which column - and therefore which key - produced it is unknowable.
    /// </para>
    /// <para>
    /// The bytes themselves are what is tested, rather than the statement, because the statement is a
    /// <c>SELECT</c> and reveals nothing about what its columns hold. That makes this a warning and never an
    /// error: the check is a strong hint, not proof, and a genuinely binary column could match it.
    /// </para>
    /// </remarks>
    private static void WarnIfScalarIsCiphertext(object? result)
    {
        if (result is byte[] bytes && EncryptionService.LooksEncrypted(bytes))
        {
            Logger.LogWarning(
                Resources.KmsEncryptionPlugin_WarnIfScalarIsCiphertext_ScalarLooksEncrypted,
                bytes.Length);
        }
    }

    /// <summary>
    /// Logs a warning for a statement this plugin cannot encrypt, then allows it to run unchanged.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Two kinds of case reach here. Most are writes whose value cannot be intercepted - a literal in the
    /// SQL text, an expression the server evaluates, an unnamed placeholder, or a statement shape whose
    /// columns cannot be matched to parameters. Those store readable data in an encrypted column. The rest
    /// are comparisons against an encrypted column, as in "WHERE ssn = @ssn", which expose nothing but can
    /// never match a row, because every value is encrypted with a fresh single-use number and so the same
    /// input never produces the same stored bytes twice.
    /// </para>
    /// <para>
    /// Neither is refused, which means the driver alone does not guarantee that an encrypted column only
    /// ever holds ciphertext. A CHECK constraint or trigger on the column is what provides that guarantee,
    /// and the plugin documentation describes how to add one for PostgreSQL and MySQL.
    /// </para>
    /// </remarks>
    private void WarnIfUnsupported(
        StatementEncryptionPlan plan,
        string methodName,
        DbParameterCollection parameters)
    {
        if (plan.Limitations.Count > 0)
        {
            Logger.LogWarning(
                Resources.KmsEncryptionPlugin_WarnIfUnsupported_RunningUnchanged,
                string.Join("; ", plan.Limitations.Select(u => $"{u.Subject} - {u.Reason}")));
        }

        // Unnamed placeholders cannot be matched to the collection: the collection's order is not the
        // order the placeholders appear in the SQL, and no driver exposes the placeholder order.
        foreach (DbParameter parameter in parameters)
        {
            if (ParameterSubstitution.NormaliseName(parameter.ParameterName).Length == 0)
            {
                Logger.LogWarning(Resources.KmsEncryptionPlugin_WarnIfUnsupported_UnnamedParameter);
                break;
            }
        }
    }

    /// <summary>
    /// Substitutes every command of a batch, runs it, and restores them all afterwards.
    /// </summary>
    /// <remarks>
    /// Each command is planned from its own command text and substituted into its own parameter collection,
    /// so a value is never matched against another command's columns. Substitutions are collected as they
    /// are made and restored on every path out, including when one command fails part way through and the
    /// earlier ones have already been altered.
    /// </remarks>
    private async Task<T> ExecuteBatchAsync<T>(DbBatch batch, string methodName, ADONetDelegate<T> methodFunc)
    {
        var applied = new List<ParameterSubstitution>();
        try
        {
            foreach (DbBatchCommand batchCommand in batch.BatchCommands)
            {
                if (string.IsNullOrEmpty(batchCommand.CommandText))
                {
                    continue;
                }

                StatementEncryptionPlan plan = await this.Planner
                    .PlanAsync(batchCommand.CommandText, CancellationToken.None)
                    .ConfigureAwait(false);

                if (plan.IsNoOp)
                {
                    continue;
                }

                this.WarnIfUnsupported(plan, methodName, batchCommand.Parameters);

                if (plan.ParametersToEncrypt.Count == 0)
                {
                    continue;
                }

                // Without this there is no way to make a substitute of the driver's own type, and writing
                // to the application's own parameter is not an option - see ParameterSubstitution.
                if (!batchCommand.CanCreateParameter)
                {
                    Logger.LogWarning(
                        Resources.KmsEncryptionPlugin_ExecuteBatchAsync_CannotCreateParameter);
                    continue;
                }

                applied.Add(await ParameterSubstitution
                    .ApplyAsync(
                        batchCommand, plan.ParametersToEncrypt, this.Encryptor, CancellationToken.None)
                    .ConfigureAwait(false));
            }

            T result = await methodFunc().ConfigureAwait(false);
            return await this.DecryptResultIfNeededAsync(result).ConfigureAwait(false);
        }
        finally
        {
            // Runs whether a substitution failed part way through or the batch itself threw, so the
            // commands already altered are always put back. Each restore swallows its own failure, so
            // one command cannot stop the rest being restored.
            foreach (ParameterSubstitution substitution in applied)
            {
                substitution.Dispose();
            }
        }
    }
}
