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

using System.Security.Cryptography;
using AwsWrapperDataProvider.Plugin.KmsEncryption.Properties;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

/// <summary>
/// Encrypts and decrypts individual column values with AES-GCM, using two keys per column: a data key
/// that performs the encryption and an HMAC key that signs the result.
/// </summary>
/// <remarks>
/// <para>
/// <b>Stored layout.</b> Every encrypted value is laid out as:
/// </para>
/// <code>
/// [HMAC-SHA256: 32][type marker: 1][nonce: 12][ciphertext: n][GCM tag: 16]`
/// </code>
/// <para>
/// <b>Why two keys.</b> AES-GCM's own tag authenticates the ciphertext but not the bytes stored beside
/// it, so on its own it would leave the type marker and nonce unprotected - and an altered type marker
/// makes decryption reinterpret the value as a different type. The HMAC covers everything from the type
/// marker onwards and closes that gap. (Passing the type marker to AES-GCM as associated data would
/// achieve the same with a single key; two keys are used because this is the layout the AWS Advanced JDBC
/// Wrapper already writes, and matching it lets either driver read the same column.)
/// </para>
/// <para>
/// <b>This layout is stored data and must not change.</b> Any change to the field order, sizes, or the
/// type-marker numbering makes existing encrypted columns unreadable.
/// </para>
/// </remarks>
internal sealed class EncryptionService
{
    private const int NonceLength = 12;
    private const int GcmTagLength = 16;
    private const int HmacTagLength = 32;
    private const int MarkerLength = 1;

    /// <summary>Offset of the type marker; everything from here on is covered by the HMAC.</summary>
    private const int SignedRegionOffset = HmacTagLength;

    /// <summary>
    /// The shortest a stored value can be. Even an empty value encrypts to exactly this length, so
    /// anything shorter was never produced by this plugin.
    /// </summary>
    internal const int MinimumStoredLength = HmacTagLength + MarkerLength + NonceLength + GcmTagLength;

    /// <summary>
    /// Encrypts a value. Returns <see langword="null"/> for a <see langword="null"/> input so that a SQL
    /// NULL stays NULL rather than becoming an encrypted empty value.
    /// </summary>
    /// <param name="value">The value to encrypt.</param>
    /// <param name="dataKey">The AES key for this column.</param>
    /// <param name="hmacKey">The HMAC key for this column.</param>
    /// <param name="algorithm">The algorithm named in the column's encryption metadata.</param>
    /// <returns>The bytes to store, or <see langword="null"/>.</returns>
    internal byte[]? Encrypt(object? value, byte[] dataKey, byte[] hmacKey, string algorithm)
    {
        if (value is null)
        {
            return null;
        }

        ValidateKeys(dataKey, hmacKey, algorithm);

        TypeMarker marker = ValueSerializer.MarkerFor(value);
        byte[] plaintext = ValueSerializer.Serialize(value);

        // A byte[] value is the caller's own array, so it must not be scrubbed below.
        bool plaintextIsCallerOwned = ReferenceEquals(plaintext, value);

        byte[] nonce = new byte[NonceLength];
        RandomNumberGenerator.Fill(nonce);

        byte[] stored = new byte[
            HmacTagLength + MarkerLength + NonceLength + plaintext.Length + GcmTagLength];

        try
        {
            Span<byte> storedSpan = stored;
            storedSpan[SignedRegionOffset] = (byte)marker;
            nonce.CopyTo(storedSpan[(SignedRegionOffset + MarkerLength)..]);

            Span<byte> ciphertext = storedSpan.Slice(
                SignedRegionOffset + MarkerLength + NonceLength,
                plaintext.Length);
            Span<byte> gcmTag = storedSpan[^GcmTagLength..];

            using (var aesGcm = new AesGcm(dataKey, GcmTagLength))
            {
                aesGcm.Encrypt(nonce, plaintext, ciphertext, gcmTag);
            }

            // Sign the marker, nonce, ciphertext and GCM tag together.
            using var hmac = new HMACSHA256(hmacKey);
            if (!hmac.TryComputeHash(storedSpan[SignedRegionOffset..], storedSpan[..HmacTagLength], out _))
            {
                throw new EncryptionException(Resources.EncryptionService_Encrypt_HmacFailed);
            }

            return stored;
        }
        catch (Exception ex) when (ex is not EncryptionException)
        {
            throw new EncryptionException(Resources.EncryptionService_Encrypt_Failed, ex);
        }
        finally
        {
            if (!plaintextIsCallerOwned)
            {
                CryptographicOperations.ZeroMemory(plaintext);
            }
        }
    }

    /// <summary>
    /// Decrypts a stored value, returning it as the type recorded when it was encrypted.
    /// </summary>
    /// <param name="stored">The bytes read from the column.</param>
    /// <param name="dataKey">The AES key for this column.</param>
    /// <param name="hmacKey">The HMAC key for this column.</param>
    /// <param name="algorithm">The algorithm named in the column's encryption metadata.</param>
    /// <param name="column">
    /// The column being read, used only to make failure messages identify where to look.
    /// </param>
    /// <returns>The original value, or <see langword="null"/> if <paramref name="stored"/> is null.</returns>
    /// <exception cref="EncryptionException">
    /// Thrown when the value is malformed, fails its integrity check, or cannot be decrypted.
    /// </exception>
    internal object? Decrypt(
        byte[]? stored,
        byte[] dataKey,
        byte[] hmacKey,
        string algorithm,
        string column = "an encrypted column")
    {
        if (stored is null)
        {
            return null;
        }

        ValidateKeys(dataKey, hmacKey, algorithm);

        if (stored.Length < MinimumStoredLength)
        {
            // Conclusive: even an empty value encrypts to exactly the minimum length, so anything shorter
            // was never produced by this plugin.
            throw EncryptionException.NotEncrypted(column, stored.Length, MinimumStoredLength);
        }

        ReadOnlySpan<byte> storedSpan = stored;
        ReadOnlySpan<byte> expectedHmac = storedSpan[..HmacTagLength];
        ReadOnlySpan<byte> signedRegion = storedSpan[SignedRegionOffset..];

        // Verify integrity before touching anything else, so a tampered type marker can never reach
        // deserialization. FixedTimeEquals keeps the comparison free of timing signal.
        Span<byte> actualHmac = stackalloc byte[HmacTagLength];
        using (var hmac = new HMACSHA256(hmacKey))
        {
            if (!hmac.TryComputeHash(signedRegion, actualHmac, out _))
            {
                throw new EncryptionException(Resources.EncryptionService_Decrypt_HmacFailed);
            }
        }

        if (!CryptographicOperations.FixedTimeEquals(expectedHmac, actualHmac))
        {
            throw EncryptionException.IntegrityCheckFailed(column);
        }

        var marker = (TypeMarker)storedSpan[SignedRegionOffset];
        ReadOnlySpan<byte> nonce = storedSpan.Slice(SignedRegionOffset + MarkerLength, NonceLength);
        int ciphertextLength = stored.Length - MinimumStoredLength;
        ReadOnlySpan<byte> ciphertext = storedSpan.Slice(
            SignedRegionOffset + MarkerLength + NonceLength,
            ciphertextLength);
        ReadOnlySpan<byte> gcmTag = storedSpan[^GcmTagLength..];

        byte[] plaintext = new byte[ciphertextLength];
        bool plaintextReturnedToCaller = marker == TypeMarker.ByteArray;
        try
        {
            using (var aesGcm = new AesGcm(dataKey, GcmTagLength))
            {
                aesGcm.Decrypt(nonce, ciphertext, gcmTag, plaintext);
            }

            return ValueSerializer.Deserialize(plaintext, marker);
        }
        catch (AuthenticationTagMismatchException ex)
        {
            throw new EncryptionException(Resources.EncryptionService_Decrypt_WrongKey, ex);
        }
        catch (Exception ex) when (ex is not EncryptionException)
        {
            throw new EncryptionException(Resources.EncryptionService_Decrypt_Failed, ex);
        }
        finally
        {
            // For a byte[] column the decrypted buffer *is* the returned value, so scrubbing it here
            // would hand the caller an array of zeros.
            if (!plaintextReturnedToCaller)
            {
                CryptographicOperations.ZeroMemory(plaintext);
            }
        }
    }

    /// <summary>
    /// Returns whether these bytes have the shape of a value this plugin stored.
    /// </summary>
    /// <remarks>
    /// Only the length and the type marker are checked, because this is used where no key is available and
    /// so the signature cannot be verified. That makes it a strong hint rather than proof, which is why it
    /// only ever drives a warning and never a decision about what to store.
    /// </remarks>
    internal static bool LooksEncrypted(byte[]? stored) =>
        stored is not null
        && stored.Length >= MinimumStoredLength
        && Enum.IsDefined(typeof(TypeMarker), stored[HmacTagLength]);

    private static void ValidateKeys(byte[] dataKey, byte[] hmacKey, string algorithm)
    {
        ArgumentNullException.ThrowIfNull(dataKey);
        ArgumentNullException.ThrowIfNull(hmacKey);

        int expected = EncryptionAlgorithm.GetKeyLength(algorithm);
        if (dataKey.Length != expected)
        {
            throw EncryptionException.InvalidKeyLength(algorithm, "data key", expected, dataKey.Length);
        }

        if (hmacKey.Length == 0)
        {
            throw EncryptionException.InvalidKeyLength(algorithm, "HMAC key", HmacTagLength, hmacKey.Length);
        }
    }
}
