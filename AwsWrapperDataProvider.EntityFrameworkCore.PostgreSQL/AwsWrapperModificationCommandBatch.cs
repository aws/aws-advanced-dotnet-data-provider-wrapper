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
using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Npgsql;

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL;

/// <summary>
///     A wrapper-aware modification command batch that handles the case where the
///     <see cref="DbDataReader"/> is not directly an <see cref="NpgsqlDataReader"/>,
///     but wraps one (e.g. via <c>AwsWrapperDataReader</c>).
/// </summary>
/// <remarks>
///     This is a near-copy of <c>NpgsqlModificationCommandBatch</c> from efcore.pg,
///     with the only change being the use of <see cref="UnwrapNpgsqlDataReader"/> to
///     safely obtain the underlying <see cref="NpgsqlDataReader"/> for per-statement
///     rows-affected checks.
///     See https://github.com/npgsql/efcore.pg/issues/1922.
/// </remarks>
public class AwsWrapperModificationCommandBatch : ReaderModificationCommandBatch
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="AwsWrapperModificationCommandBatch" /> class.
    /// </summary>
    public AwsWrapperModificationCommandBatch(
        ModificationCommandBatchFactoryDependencies dependencies,
        int maxBatchSize)
        : base(dependencies)
    {
        this.MaxBatchSize = maxBatchSize;
    }

    /// <inheritdoc />
    protected override int MaxBatchSize { get; }

    /// <inheritdoc />
    protected override void AddParameter(IColumnModification columnModification)
    {
        if (columnModification.Column is IStoreStoredProcedureParameter { Direction: ParameterDirection.Output })
        {
            return;
        }

        base.AddParameter(columnModification);
    }

    /// <inheritdoc />
    protected override void Consume(RelationalDataReader reader)
        => this.ConsumeInternal(reader, async: false).GetAwaiter().GetResult();

    /// <inheritdoc />
    protected override Task ConsumeAsync(RelationalDataReader reader, CancellationToken cancellationToken = default)
        => this.ConsumeInternal(reader, async: true, cancellationToken);

    private async Task ConsumeInternal(RelationalDataReader reader, bool async, CancellationToken cancellationToken = default)
    {
        var npgsqlReader = UnwrapNpgsqlDataReader(reader.DbDataReader);

#pragma warning disable 618
        Debug.Assert(
            npgsqlReader is null || npgsqlReader.Statements.Count == this.ModificationCommands.Count,
            $"Reader has {npgsqlReader?.Statements.Count} statements, expected {this.ModificationCommands.Count}");
#pragma warning restore 618

        var commandIndex = 0;

        try
        {
            bool? onResultSet = null;
            while (commandIndex < this.ModificationCommands.Count)
            {
                var command = this.ModificationCommands[commandIndex];

                if (this.ResultSetMappings[commandIndex].HasFlag(ResultSetMapping.HasResultRow))
                {
                    if (async)
                    {
                        if (!(await reader.ReadAsync(cancellationToken).ConfigureAwait(false)))
                        {
                            await this.ThrowAggregateUpdateConcurrencyExceptionAsync(reader, commandIndex, 1, 0, cancellationToken)
                                .ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        if (!reader.Read())
                        {
                            this.ThrowAggregateUpdateConcurrencyException(reader, commandIndex, 1, 0);
                        }
                    }

                    if (command.RowsAffectedColumn is { } rowsAffectedColumn)
                    {
                        Debug.Assert(command.StoreStoredProcedure is not null);

                        var rowsAffectedParameter = (IStoreStoredProcedureParameter)rowsAffectedColumn;
                        Debug.Assert(rowsAffectedParameter.Direction == ParameterDirection.Output);

                        var readerIndex = -1;

                        for (var i = 0; i < command.ColumnModifications.Count; i++)
                        {
                            var columnModification = command.ColumnModifications[i];
                            if (columnModification.Column is IStoreStoredProcedureParameter
                                {
                                    Direction: ParameterDirection.Output or ParameterDirection.InputOutput
                                })
                            {
                                readerIndex++;
                            }

                            if (columnModification.Column == rowsAffectedColumn)
                            {
                                break;
                            }
                        }

                        if (reader.DbDataReader.GetInt32(readerIndex) != 1)
                        {
                            await this.ThrowAggregateUpdateConcurrencyExceptionAsync(reader, commandIndex, 1, 0, cancellationToken)
                                .ConfigureAwait(false);
                        }
                    }

                    command.PropagateResults(reader);

                    commandIndex++;

                    onResultSet = async
                        ? await reader.DbDataReader.NextResultAsync(cancellationToken).ConfigureAwait(false)
                        : reader.DbDataReader.NextResult();
                }
                else
                {
                    Debug.Assert(this.ResultSetMappings[commandIndex] == ResultSetMapping.NoResults);

#pragma warning disable 618
                    if (npgsqlReader is not null
                        && npgsqlReader.Statements[commandIndex].Rows != 1
                        && command.StoreStoredProcedure is null)
                    {
                        if (async)
                        {
                            await this.ThrowAggregateUpdateConcurrencyExceptionAsync(reader, commandIndex, 1, 0, cancellationToken)
                                .ConfigureAwait(false);
                        }
                        else
                        {
                            this.ThrowAggregateUpdateConcurrencyException(reader, commandIndex, 1, 0);
                        }
                    }
#pragma warning restore 618
                    commandIndex++;
                }
            }

            if (onResultSet == true)
            {
                this.Dependencies.UpdateLogger.UnexpectedTrailingResultSetWhenSaving();
            }
        }
        catch (Exception ex) when (ex is not DbUpdateException and not OperationCanceledException)
        {
            if (commandIndex == this.ModificationCommands.Count)
            {
                commandIndex--;
            }

            throw new DbUpdateException(
                RelationalStrings.UpdateStoreException,
                ex,
                this.ModificationCommands[commandIndex].Entries);
        }
    }

    /// <summary>
    ///     Attempts to unwrap the underlying <see cref="NpgsqlDataReader"/> from a wrapper reader.
    ///     Returns <c>null</c> if the reader is neither an <see cref="NpgsqlDataReader"/> nor wraps one.
    /// </summary>
    private static NpgsqlDataReader? UnwrapNpgsqlDataReader(DbDataReader dataReader)
    {
        if (dataReader is NpgsqlDataReader npgsqlReader)
        {
            return npgsqlReader;
        }

        // Support wrapper readers that expose an Unwrap<T>() method (e.g. AwsWrapperDataReader).
        var unwrapMethod = dataReader.GetType().GetMethod("Unwrap");
        if (unwrapMethod is not null && unwrapMethod.IsGenericMethodDefinition)
        {
            try
            {
                return unwrapMethod.MakeGenericMethod(typeof(NpgsqlDataReader)).Invoke(dataReader, null) as NpgsqlDataReader;
            }
            catch
            {
                // Unwrap failed — the inner reader is not an NpgsqlDataReader.
            }
        }

        return null;
    }

    private IReadOnlyList<IUpdateEntry> AggregateEntries(int endIndex, int commandCount)
    {
        var entries = new List<IUpdateEntry>();
        for (var i = endIndex - commandCount; i < endIndex; i++)
        {
            entries.AddRange(this.ModificationCommands[i].Entries);
        }

        return entries;
    }

    /// <inheritdoc cref="Npgsql.EntityFrameworkCore.PostgreSQL.Update.Internal.NpgsqlModificationCommandBatch" />
    protected virtual void ThrowAggregateUpdateConcurrencyException(
        RelationalDataReader reader,
        int commandIndex,
        int expectedRowsAffected,
        int rowsAffected)
    {
        var entries = this.AggregateEntries(commandIndex + 1, expectedRowsAffected);
        var exception = new DbUpdateConcurrencyException(
            RelationalStrings.UpdateConcurrencyException(expectedRowsAffected, rowsAffected),
            entries);

        if (!this.Dependencies.UpdateLogger.OptimisticConcurrencyException(
                this.Dependencies.CurrentContext.Context,
                entries,
                exception,
                (c, ex, e, d) => CreateConcurrencyExceptionEventData(c, reader, ex, e, d)).IsSuppressed)
        {
            throw exception;
        }
    }

    /// <inheritdoc cref="Npgsql.EntityFrameworkCore.PostgreSQL.Update.Internal.NpgsqlModificationCommandBatch" />
    protected virtual async Task ThrowAggregateUpdateConcurrencyExceptionAsync(
        RelationalDataReader reader,
        int commandIndex,
        int expectedRowsAffected,
        int rowsAffected,
        CancellationToken cancellationToken)
    {
        var entries = this.AggregateEntries(commandIndex + 1, expectedRowsAffected);
        var exception = new DbUpdateConcurrencyException(
            RelationalStrings.UpdateConcurrencyException(expectedRowsAffected, rowsAffected),
            entries);

        if (!(await this.Dependencies.UpdateLogger.OptimisticConcurrencyExceptionAsync(
                    this.Dependencies.CurrentContext.Context,
                    entries,
                    exception,
                    (c, ex, e, d) => CreateConcurrencyExceptionEventData(c, reader, ex, e, d),
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false)).IsSuppressed)
        {
            throw exception;
        }
    }

    private static RelationalConcurrencyExceptionEventData CreateConcurrencyExceptionEventData(
        DbContext context,
        RelationalDataReader reader,
        DbUpdateConcurrencyException exception,
        IReadOnlyList<IUpdateEntry> entries,
        EventDefinition<Exception> definition)
        => new(
            definition,
            (definition1, payload)
                => ((EventDefinition<Exception>)definition1).GenerateMessage(((ConcurrencyExceptionEventData)payload).Exception),
            context,
            reader.RelationalConnection.DbConnection,
            reader.DbCommand,
            reader.DbDataReader,
            reader.CommandId,
            reader.RelationalConnection.ConnectionId,
            entries,
            exception);
}
