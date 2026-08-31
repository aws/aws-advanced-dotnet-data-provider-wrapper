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

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

/// <summary>
/// The encryption algorithms recognised in the encryption metadata, and the data key length each
/// requires. The names are the values stored in the metadata's <c>encryption_algorithm</c> column.
/// </summary>
internal static class EncryptionAlgorithm
{
    internal const string Aes256Gcm = "AES-256-GCM";
    internal const string Aes128Gcm = "AES-128-GCM";

    private static readonly Dictionary<string, int> KeyLengthsByName = new(StringComparer.Ordinal)
    {
        { Aes256Gcm, 32 },
        { Aes128Gcm, 16 },
    };

    /// <summary>Returns whether the given algorithm name is supported.</summary>
    internal static bool IsSupported(string? algorithm) =>
        algorithm != null && KeyLengthsByName.ContainsKey(algorithm);

    /// <summary>
    /// Returns the data key length, in bytes, required by the given algorithm.
    /// </summary>
    /// <exception cref="EncryptionException">Thrown when the algorithm is not supported.</exception>
    internal static int GetKeyLength(string? algorithm)
    {
        if (algorithm == null || !KeyLengthsByName.TryGetValue(algorithm, out int length))
        {
            throw EncryptionException.UnsupportedAlgorithm(algorithm);
        }

        return length;
    }
}
