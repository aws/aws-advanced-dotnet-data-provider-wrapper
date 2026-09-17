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

using System.Globalization;
using System.Security.Cryptography;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Key;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;
using AwsWrapperDataProvider.Plugin.KmsEncryption.Properties;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

/// <summary>
/// Encrypts and decrypts values for a column, bringing together the key material for that column and the
/// encryption itself.
/// </summary>
internal sealed class ColumnEncryptor : IColumnEncryptor
{
    private readonly KeyManager keyManager;
    private readonly EncryptionService encryptionService;

    internal ColumnEncryptor(KeyManager keyManager, EncryptionService encryptionService)
    {
        this.keyManager = keyManager;
        this.encryptionService = encryptionService;
    }

    public async Task<byte[]> EncryptAsync(
        object value,
        ColumnEncryptionConfig column,
        CancellationToken cancellationToken)
    {
        DataKeyMetadata keyMetadata = Require(column);
        byte[] dataKey = await this.keyManager
            .GetDataKeyAsync(keyMetadata, cancellationToken)
            .ConfigureAwait(false);

        try
        {
            byte[]? encrypted = this.encryptionService.Encrypt(
                value,
                dataKey,
                keyMetadata.HmacKey,
                column.Algorithm);

            // Encrypt only returns null for a null value, and callers never pass one: a SQL NULL is left
            // as NULL before it reaches here.
            return encrypted ?? throw new EncryptionException(string.Format(
                CultureInfo.CurrentCulture,
                Resources.ColumnEncryptor_EncryptAsync_NoValueProduced,
                column.ColumnIdentifier));
        }
        finally
        {
            // The key is a private copy handed over for this operation only, so it is scrubbed as soon as
            // the operation ends rather than left in the heap until collection.
            CryptographicOperations.ZeroMemory(dataKey);
        }
    }

    /// <summary>Decrypts a stored value for <paramref name="column"/>.</summary>
    internal async Task<object?> DecryptAsync(
        byte[]? stored,
        ColumnEncryptionConfig column,
        CancellationToken cancellationToken)
    {
        if (stored is null)
        {
            return null;
        }

        DataKeyMetadata keyMetadata = Require(column);
        byte[] dataKey = await this.keyManager
            .GetDataKeyAsync(keyMetadata, cancellationToken)
            .ConfigureAwait(false);

        try
        {
            return this.encryptionService.Decrypt(
                stored,
                dataKey,
                keyMetadata.HmacKey,
                column.Algorithm,
                column.ColumnIdentifier);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(dataKey);
        }
    }

    private static DataKeyMetadata Require(ColumnEncryptionConfig column) =>
        column.DataKeyMetadata ?? throw new EncryptionException(string.Format(
            CultureInfo.CurrentCulture,
            Resources.ColumnEncryptor_Require_NoKeyMaterial,
            column.ColumnIdentifier,
            column.KeyId));
}
