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

using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

/// <summary>
/// Works out what a statement requires, from its SQL and the encryption metadata.
/// </summary>
/// <remarks>
/// Kept behind an interface so the interception mechanism can be built and tested independently of SQL
/// analysis and metadata lookup.
/// </remarks>
internal interface IStatementEncryptionPlanner
{
    /// <summary>Builds the plan for <paramref name="commandText"/>.</summary>
    Task<StatementEncryptionPlan> PlanAsync(string commandText, CancellationToken cancellationToken);
}
