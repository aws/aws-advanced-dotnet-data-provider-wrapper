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
using AwsWrapperDataProvider.Plugin.KmsEncryption.Properties;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

/// <summary>
/// Thrown when a value cannot be encrypted or decrypted.
/// </summary>
/// <remarks>
/// Messages deliberately never include plaintext, key material, or ciphertext, so that a failure can
/// be logged without leaking the data the plugin exists to protect.
/// </remarks>
public class EncryptionException : Exception
{
    public EncryptionException(string message)
        : base(message)
    {
    }

    public EncryptionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    internal static EncryptionException UnsupportedAlgorithm(string? algorithm) =>
        new(string.Format(
            CultureInfo.CurrentCulture,
            Resources.EncryptionException_UnsupportedAlgorithm,
            algorithm ?? "<null>",
            EncryptionAlgorithm.Aes256Gcm,
            EncryptionAlgorithm.Aes128Gcm));

    internal static EncryptionException InvalidKeyLength(string algorithm, string keyName, int expected, int actual) =>
        new(string.Format(
            CultureInfo.CurrentCulture,
            Resources.EncryptionException_InvalidKeyLength,
            keyName,
            algorithm,
            expected,
            actual));

    internal static EncryptionException UnsupportedType(Type type) =>
        new(string.Format(
            CultureInfo.CurrentCulture,
            Resources.EncryptionException_UnsupportedType,
            type.FullName));

    internal static EncryptionException Malformed(string detail) =>
        new(string.Format(CultureInfo.CurrentCulture, Resources.EncryptionException_Malformed, detail));

    /// <summary>
    /// Reports a value too short to be an encrypted value of any length. Since the shortest possible
    /// encrypted value is <paramref name="minimumLength"/> bytes, this is conclusive rather than a guess,
    /// so the message leads with the overwhelmingly likely cause: the value was written without the
    /// plugin. That happens when the plugin was not enabled, when the value was written as a literal in
    /// the statement instead of as a parameter, or when another application or an administrative tool
    /// wrote the row.
    /// </summary>
    internal static EncryptionException NotEncrypted(string column, int actualLength, int minimumLength) =>
        new(string.Format(
            CultureInfo.CurrentCulture,
            Resources.EncryptionException_NotEncrypted,
            column,
            actualLength,
            minimumLength));

    /// <summary>
    /// Reports a failed integrity check. All three plausible causes are named, because the message is the
    /// only signal an operator gets and the first cause is by far the most common.
    /// </summary>
    internal static EncryptionException IntegrityCheckFailed(string column) =>
        new(string.Format(
            CultureInfo.CurrentCulture,
            Resources.EncryptionException_IntegrityCheckFailed,
            column));
}
