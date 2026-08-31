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

using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.KmsEncryption;

/// <summary>
/// Builds a <see cref="KmsEncryptionPlugin"/> with stub collaborators, for tests that only care about the
/// plugin taking part in the chain.
/// <para>
/// The production factory deliberately throws until metadata lookup and key management are wired up,
/// because a plugin that silently did nothing would let an application write readable values into a
/// column it believes is encrypted. Tests that need an instance therefore supply their own factory
/// rather than the production one being made permissive.
/// </para>
/// </summary>
internal sealed class TestKmsEncryptionPluginFactory : IConnectionPluginFactory
{
    public IConnectionPlugin GetInstance(FullServicesContainer servicesContainer, Dictionary<string, string> props)
    {
        return new KmsEncryptionPlugin(
            servicesContainer.PluginService,
            props,
            new NoEncryptedColumnsPlanner(),
            new UnusedColumnEncryptor());
    }

    /// <summary>Reports that no statement involves an encrypted column.</summary>
    private sealed class NoEncryptedColumnsPlanner : IStatementEncryptionPlanner
    {
        public Task<StatementEncryptionPlan> PlanAsync(string commandText, CancellationToken cancellationToken)
            => Task.FromResult(StatementEncryptionPlan.None);
    }

    /// <summary>Never invoked, because the planner above never asks for encryption.</summary>
    private sealed class UnusedColumnEncryptor : IColumnEncryptor
    {
        public Task<byte[]> EncryptAsync(object value, ColumnEncryptionConfig column, CancellationToken cancellationToken)
            => throw new InvalidOperationException("The planner reported nothing to encrypt.");
    }
}
