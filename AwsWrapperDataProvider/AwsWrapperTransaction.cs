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

namespace AwsWrapperDataProvider;

public class AwsWrapperTransaction : DbTransaction
{
    protected ConnectionPluginManager pluginManager;
    protected IPluginService pluginService;
    protected AwsWrapperConnection wrapperConnection;

    protected DbTransaction TargetTransaction => this.pluginService.CurrentTransaction
                                                 ?? throw new ArgumentNullException(nameof(this.pluginService.CurrentTransaction));

    internal AwsWrapperTransaction(AwsWrapperConnection wrapperConnection, IPluginService pluginService, ConnectionPluginManager pluginManager)
    {
        this.pluginManager = pluginManager;
        this.pluginService = pluginService;
        this.wrapperConnection = wrapperConnection;
    }

    public override IsolationLevel IsolationLevel => this.TargetTransaction.IsolationLevel;

    internal DbTransaction TargetDbTransaction => this.TargetTransaction;

    protected override DbConnection? DbConnection => this.wrapperConnection;

    public override void Commit()
    {
        WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.Commit",
            () => this.TargetTransaction!.Commit());
    }

    public override void Rollback()
    {
        WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.Rollback",
            () => this.TargetTransaction!.Rollback());
    }

    public override void Save(string savepointName)
    {
        WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.Save",
            () => this.TargetTransaction!.Save(savepointName));
    }

    public override void Rollback(string savepointName)
    {
        WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.Rollback",
            () => this.TargetTransaction!.Rollback(savepointName));
    }

    public override void Release(string savepointName)
    {
        WrapperUtils.RunWithPlugins(
            this.pluginManager!,
            this.TargetTransaction!,
            "DbTransaction.Release",
            () => this.TargetTransaction!.Release(savepointName));
    }
}
