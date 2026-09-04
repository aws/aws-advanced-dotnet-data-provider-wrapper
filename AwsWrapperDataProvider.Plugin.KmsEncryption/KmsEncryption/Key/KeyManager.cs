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
using Amazon.KeyManagementService;
using Amazon.KeyManagementService.Model;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Cache;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;
using AwsWrapperDataProvider.Plugin.KmsEncryption.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Key;

/// <summary>
/// Recovers the plaintext data key for a column by asking AWS KMS to decrypt the stored, encrypted data
/// key, and caches the result so that the common path does not call KMS.
/// </summary>
/// <remarks>
/// Retries are left to the AWS SDK, which applies its own backoff policy; re-implementing that here would
/// only risk diverging from it. Concurrent requests for the same key share a single KMS call - see
/// <see cref="DataKeyCache"/>.
/// </remarks>
internal sealed class KeyManager : IDisposable
{
    private static readonly ILogger<KeyManager> Logger = LoggerUtils.GetLogger<KeyManager>();

    private readonly Lazy<IAmazonKeyManagementService> kms;
    private readonly DataKeyCache dataKeyCache;
    private readonly bool ownsKmsClient;

    /// <summary>
    /// Initializes a new instance of the <see cref="KeyManager"/> class whose client is built on first use.
    /// </summary>
    /// <remarks>
    /// The client is deferred because plugins are constructed while the connection chain is being built,
    /// before any host has been selected - so anything that needs the connected host, such as resolving
    /// application-supplied credentials, is not available yet.
    /// </remarks>
    internal KeyManager(
        Func<IAmazonKeyManagementService> kmsFactory,
        DataKeyCache dataKeyCache,
        bool ownsKmsClient = false)
    {
        this.kms = new Lazy<IAmazonKeyManagementService>(
            kmsFactory,
            LazyThreadSafetyMode.ExecutionAndPublication);
        this.dataKeyCache = dataKeyCache;
        this.ownsKmsClient = ownsKmsClient;
    }

    internal KeyManager(IAmazonKeyManagementService kms, DataKeyCache dataKeyCache, bool ownsKmsClient = false)
        : this(() => kms, dataKeyCache, ownsKmsClient)
    {
    }

    /// <summary>
    /// Returns the plaintext data key for <paramref name="keyMetadata"/>.
    /// </summary>
    /// <returns>
    /// A private copy of the data key, owned by the caller. Use it for the current operation and do not
    /// retain it: the cache scrubs its own copy when the entry leaves, and a retained reference would be a
    /// second copy of key material that nothing tracks.
    /// </returns>
    /// <exception cref="EncryptionException">Thrown when the data key cannot be recovered.</exception>
    internal Task<byte[]> GetDataKeyAsync(DataKeyMetadata keyMetadata, CancellationToken cancellationToken)
    {
        // Keyed on the encrypted data key rather than the key id, so that rotating a column's key produces
        // a different cache entry instead of silently reusing the previous plaintext.
        return this.dataKeyCache.GetOrAddAsync(
            keyMetadata.EncryptedDataKey,
            () => this.DecryptDataKeyAsync(keyMetadata, cancellationToken));
    }

    public void Dispose()
    {
        this.dataKeyCache.Dispose();
        if (this.ownsKmsClient && this.kms.IsValueCreated)
        {
            this.kms.Value.Dispose();
        }
    }

    private async Task<byte[]> DecryptDataKeyAsync(DataKeyMetadata keyMetadata, CancellationToken cancellationToken)
    {
        byte[] encrypted;
        try
        {
            encrypted = Convert.FromBase64String(keyMetadata.EncryptedDataKey);
        }
        catch (FormatException ex)
        {
            throw new EncryptionException(
                string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.KeyManager_DecryptDataKeyAsync_InvalidBase64,
                    keyMetadata.KeyId),
                ex);
        }

        Logger.LogTrace(
            Resources.KeyManager_DecryptDataKeyAsync_RequestingDecrypt,
            keyMetadata.KeyId,
            keyMetadata.MasterKeyArn);

        try
        {
            using var ciphertext = new MemoryStream(encrypted, writable: false);
            DecryptResponse response = await this.kms.Value.DecryptAsync(
                new DecryptRequest
                {
                    CiphertextBlob = ciphertext,
                    KeyId = keyMetadata.MasterKeyArn,
                },
                cancellationToken).ConfigureAwait(false);

            byte[] dataKey = response.Plaintext.ToArray();

            int expected = EncryptionAlgorithm.GetKeyLength(KeySpecToAlgorithm(keyMetadata.KeySpec));
            if (dataKey.Length != expected)
            {
                throw new EncryptionException(string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.KeyManager_DecryptDataKeyAsync_WrongLength,
                    keyMetadata.KeyId,
                    dataKey.Length,
                    keyMetadata.KeySpec,
                    expected));
            }

            return dataKey;
        }
        catch (EncryptionException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // The message deliberately names only the key, never any key material or plaintext.
            throw new EncryptionException(
                string.Format(
                    CultureInfo.CurrentCulture,
                    Resources.KeyManager_DecryptDataKeyAsync_KmsFailed,
                    keyMetadata.KeyId,
                    keyMetadata.MasterKeyArn),
                ex);
        }
    }

    /// <summary>
    /// Maps a stored key specification onto the algorithm whose key length it must match.
    /// </summary>
    private static string KeySpecToAlgorithm(string keySpec) => keySpec switch
    {
        "AES_256" => EncryptionAlgorithm.Aes256Gcm,
        "AES_128" => EncryptionAlgorithm.Aes128Gcm,
        _ => throw new EncryptionException(string.Format(
            CultureInfo.CurrentCulture,
            Resources.KeyManager_KeySpecToAlgorithm_Unsupported,
            keySpec)),
    };
}
