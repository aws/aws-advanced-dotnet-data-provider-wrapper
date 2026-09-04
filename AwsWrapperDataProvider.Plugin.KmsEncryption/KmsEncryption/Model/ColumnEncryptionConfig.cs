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

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;

/// <summary>
/// The encryption configuration recorded for a single column, as read from the encryption metadata.
/// </summary>
internal class ColumnEncryptionConfig
{
    internal ColumnEncryptionConfig(
        string table,
        string column,
        string keyId,
        string algorithm,
        DataKeyMetadata? keyMetadata = null)
    {
        this.Table = table;
        this.Column = column;
        this.KeyId = keyId;
        this.Algorithm = algorithm;
        this.DataKeyMetadata = keyMetadata;
    }

    /// <summary>Gets the name of the table the column belongs to.</summary>
    internal string Table { get; }

    /// <summary>Gets the name of the column.</summary>
    internal string Column { get; }

    /// <summary>
    /// Gets the identifier of the data key this column is encrypted with. Each column has its own key, so
    /// a value encrypted for one column cannot be read as another.
    /// </summary>
    internal string KeyId { get; }

    /// <summary>Gets the encryption algorithm recorded for this column.</summary>
    internal string Algorithm { get; }

    /// <summary>
    /// Gets the key material for <see cref="KeyId"/>, or <see langword="null"/> when the metadata names a
    /// key that is missing from the key storage table. Encryption fails rather than falling back, so a
    /// broken reference cannot result in a column being left readable.
    /// </summary>
    internal DataKeyMetadata? DataKeyMetadata { get; }

    /// <summary>Gets the column identifier, in <c>table.column</c> form.</summary>
    internal string ColumnIdentifier => $"{this.Table}.{this.Column}";

    public override string ToString() => this.ColumnIdentifier;
}
