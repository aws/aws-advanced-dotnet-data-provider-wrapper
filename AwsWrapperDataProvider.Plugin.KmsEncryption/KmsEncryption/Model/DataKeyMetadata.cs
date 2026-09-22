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
/// The key material recorded for one encryption key, as stored in the key storage table.
/// </summary>
/// <remarks>
/// The data key is held encrypted and can only be recovered by calling AWS KMS with the master key. The
/// HMAC key, by contrast, is stored as-is, so anyone who can read the key storage table can forge the
/// integrity tag on an encrypted value. That is a weaker guarantee than it may appear: the HMAC protects
/// the type marker and nonce, which sit outside the encrypted payload, against a party that can modify
/// stored data but cannot read this table. It offers no protection against a party that can read it. The
/// confidentiality of the data itself never depends on the HMAC key.
/// </remarks>
internal class DataKeyMetadata
{
    internal DataKeyMetadata(
        string keyId,
        string masterKeyArn,
        string encryptedDataKey,
        byte[] hmacKey,
        string keySpec)
    {
        this.KeyId = keyId;
        this.MasterKeyArn = masterKeyArn;
        this.EncryptedDataKey = encryptedDataKey;
        this.HmacKey = hmacKey;
        this.KeySpec = keySpec;
    }

    /// <summary>Gets the identifier of this key in the key storage table.</summary>
    internal string KeyId { get; }

    /// <summary>Gets the ARN of the KMS master key that protects the data key.</summary>
    internal string MasterKeyArn { get; }

    /// <summary>Gets the data key, encrypted under the master key, in base64.</summary>
    internal string EncryptedDataKey { get; }

    /// <summary>Gets the HMAC key used to sign encrypted values for columns using this key.</summary>
    internal byte[] HmacKey { get; }

    /// <summary>Gets the key specification recorded for the data key, such as <c>AES_256</c>.</summary>
    internal string KeySpec { get; }
}
