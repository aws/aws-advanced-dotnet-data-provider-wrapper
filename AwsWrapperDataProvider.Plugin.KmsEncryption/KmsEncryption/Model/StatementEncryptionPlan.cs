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

using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Wrapper;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;

/// <summary>
/// What the plugin must do to one statement: which parameters to encrypt, which placeholders it must
/// refuse, and whether the statement's results will contain encrypted columns.
/// </summary>
/// <remarks>
/// The whole statement is assessed once, before it runs, because the plugin has a single opportunity to
/// act on it.
/// </remarks>
internal class StatementEncryptionPlan
{
    private static readonly Dictionary<string, ColumnEncryptionConfig> NoParameters =
        new(StringComparer.Ordinal);

    private static readonly EncryptionLimitation[] NoLimitations = Array.Empty<EncryptionLimitation>();

    internal StatementEncryptionPlan(
        IReadOnlyDictionary<string, ColumnEncryptionConfig>? parametersToEncrypt = null,
        IReadOnlyList<EncryptionLimitation>? limitations = null)
    {
        this.ParametersToEncrypt = parametersToEncrypt ?? NoParameters;
        this.Limitations = limitations ?? NoLimitations;
    }

    /// <summary>Gets a plan for a statement that involves no encrypted columns at all.</summary>
    internal static StatementEncryptionPlan None { get; } = new();

    /// <summary>
    /// Gets the parameters to encrypt, keyed by normalised parameter name
    /// (see <see cref="ParameterSubstitution.NormaliseName"/>).
    /// </summary>
    internal IReadOnlyDictionary<string, ColumnEncryptionConfig> ParametersToEncrypt { get; }

    /// <summary>Gets the placeholders that must cause the call to be refused.</summary>
    internal IReadOnlyList<EncryptionLimitation> Limitations { get; }

    /// <summary>Gets a value indicating whether this statement needs no work from the plugin.</summary>
    internal bool IsNoOp => this.ParametersToEncrypt.Count == 0 && this.Limitations.Count == 0;
}
