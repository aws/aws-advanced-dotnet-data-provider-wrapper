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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;

namespace AwsWrapperDataProvider;

public class AwsWrapperTransaction : DbTransaction, IWrapper
{
    protected ConnectionPluginManager pluginManager;
    protected IPluginService pluginService;
    protected AwsWrapperConnection wrapperConnection;

    protected DbTransaction? TargetTransaction => this.pluginService.CurrentTransaction;

    internal AwsWrapperTransaction(AwsWrapperConnection wrapperConnection, IPluginService pluginService, ConnectionPluginManager pluginManager)
    {
        this.pluginManager = pluginManager;
        this.pluginService = pluginService;
        this.wrapperConnection = wrapperConnection;
    }

    public override IsolationLevel IsolationLevel => this.TargetTransaction?.IsolationLevel ?? IsolationLevel.Unspecified;

    internal DbTransaction? TargetDbTransaction => this.TargetTransaction;

    protected override DbConnection? DbConnection => this.wrapperConnection;

    public override void Commit()
    {
        WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.Commit",
            () =>
            {
                this.TargetTransaction!.Commit();
                return Task.CompletedTask;
            }).GetAwaiter().GetResult();
        this.pluginService.CurrentTransaction = null;
    }

    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        await WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.CommitAsync",
            () => this.TargetTransaction!.CommitAsync(cancellationToken),
            cancellationToken);
        this.pluginService.CurrentTransaction = null;
    }

    public override void Rollback()
    {
        WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.Rollback",
            () =>
            {
                this.TargetTransaction!.Rollback();
                return Task.CompletedTask;
            }).GetAwaiter().GetResult();
        this.pluginService.CurrentTransaction = null;
    }

    public override async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        await WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.RollbackAsync",
            () => this.TargetTransaction!.RollbackAsync(cancellationToken),
            cancellationToken);
        this.pluginService.CurrentTransaction = null;
    }

    public override void Save(string savepointName)
    {
        WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.Save",
            () =>
            {
                this.TargetTransaction!.Save(savepointName);
                return Task.CompletedTask;
            },
            savepointName).GetAwaiter().GetResult();
    }

    public override Task SaveAsync(string savepointName, CancellationToken cancellationToken = default)
    {
        return WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.SaveAsync",
            () => this.TargetTransaction!.SaveAsync(savepointName, cancellationToken),
            savepointName,
            cancellationToken);
    }

    public override void Rollback(string savepointName)
    {
        WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.Rollback",
            () =>
            {
                this.TargetTransaction!.Rollback(savepointName);
                return Task.CompletedTask;
            },
            savepointName).GetAwaiter().GetResult();
    }

    public override Task RollbackAsync(string savepointName, CancellationToken cancellationToken = default)
    {
        return WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.RollbackAsync",
            () => this.TargetTransaction!.RollbackAsync(savepointName, cancellationToken),
            savepointName,
            cancellationToken);
    }

    public override void Release(string savepointName)
    {
        WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.Release",
            () =>
            {
                this.TargetTransaction!.Release(savepointName);
                return Task.CompletedTask;
            },
            savepointName).GetAwaiter().GetResult();
    }

    public override Task ReleaseAsync(string savepointName, CancellationToken cancellationToken = default)
    {
        return WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.ReleaseAsync",
            () => this.TargetTransaction!.ReleaseAsync(savepointName, cancellationToken),
            savepointName,
            cancellationToken);
    }

    protected override void Dispose(bool disposing)
    {
        if (this.TargetTransaction == null)
        {
            return;
        }

        if (disposing)
        {
            this.TargetTransaction.Dispose();
            this.pluginService.CurrentTransaction = null;
        }
    }

    public override async ValueTask DisposeAsync()
    {
        if (this.TargetTransaction is not null)
        {
            await this.TargetTransaction.DisposeAsync().ConfigureAwait(false);
        }
    }

    public T Unwrap<T>() where T : class
    {
        if (this.TargetTransaction is T transactionAsT)
        {
            return transactionAsT;
        }

        throw new ArgumentException(string.Format(Resources.Error_CannotUnwrap, typeof(AwsWrapperTransaction).Name, typeof(T).Name));
    }

    public bool IsWrapperFor<T>() where T : class
    {
        return this.TargetTransaction is T;
    }
}
