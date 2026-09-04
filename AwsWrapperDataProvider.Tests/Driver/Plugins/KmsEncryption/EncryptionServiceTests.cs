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

using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.KmsEncryption;

public class EncryptionServiceTests
{
    private const string Algorithm = EncryptionAlgorithmNames.Aes256Gcm;

    private static byte[] DataKey() => Enumerable.Range(0, 32).Select(i => (byte)i).ToArray();

    private static byte[] HmacKey() => Enumerable.Range(0, 32).Select(i => (byte)(0xA0 + i)).ToArray();

    /// <summary>Mirrors the internal algorithm names, which the test assembly can see but xUnit cannot use in attributes.</summary>
    private static class EncryptionAlgorithmNames
    {
        internal const string Aes256Gcm = "AES-256-GCM";
        internal const string Aes128Gcm = "AES-128-GCM";
    }

    public static IEnumerable<object[]> RoundTripValues() => new List<object[]>
    {
        new object[] { "123-45-6789" },
        new object[] { string.Empty },
        new object[] { "unicode: é中文\U0001f512" },
        new object[] { 1234567 },
        new object[] { int.MinValue },
        new object[] { int.MaxValue },
        new object[] { 1234567890123L },
        new object[] { long.MinValue },
        new object[] { 1234.5678d },
        new object[] { double.NegativeInfinity },
        new object[] { 12.34f },
        new object[] { true },
        new object[] { false },
        new object[] { 123.4500m },
        new object[] { decimal.MinValue },
        new object[] { new DateOnly(2026, 1, 1) },
        new object[] { new TimeOnly(10, 15, 30, 123) },
        new object[] { new TimeOnly(10, 15) },
        new object[] { new DateTime(2026, 1, 1, 10, 15, 30, DateTimeKind.Unspecified) },
        new object[] { DateTimeOffset.FromUnixTimeMilliseconds(1767225600000L) },
        new object[] { new byte[] { 0xDE, 0xAD, 0xBE, 0xEF } },
        new object[] { Array.Empty<byte>() },
    };

    [Theory]
    [Trait("Category", "Unit")]
    [MemberData(nameof(RoundTripValues))]
    public void TestValueSurvivesARoundTrip(object value)
    {
        var service = new EncryptionService();

        byte[]? stored = service.Encrypt(value, DataKey(), HmacKey(), Algorithm);
        Assert.NotNull(stored);

        object? recovered = service.Decrypt(stored, DataKey(), HmacKey(), Algorithm);

        Assert.Equal(value, recovered);
        Assert.Equal(value.GetType(), recovered!.GetType());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestStoredLayoutHasTheExpectedSizeAndMarker()
    {
        var service = new EncryptionService();
        byte[] stored = service.Encrypt("123-45-6789", DataKey(), HmacKey(), Algorithm)!;

        // [HMAC 32][marker 1][nonce 12][ciphertext 11][GCM tag 16]
        Assert.Equal(32 + 1 + 12 + 11 + 16, stored.Length);
        Assert.Equal((byte)TypeMarker.String, stored[32]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestNonceIsFreshPerCallSoIdenticalValuesDifferOnDisk()
    {
        var service = new EncryptionService();
        byte[] first = service.Encrypt("same", DataKey(), HmacKey(), Algorithm)!;
        byte[] second = service.Encrypt("same", DataKey(), HmacKey(), Algorithm)!;

        // Equal ciphertext for equal plaintext would let an observer match rows without decrypting.
        Assert.NotEqual(first, second);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestNullValueStaysNull()
    {
        var service = new EncryptionService();
        Assert.Null(service.Encrypt(null, DataKey(), HmacKey(), Algorithm));
        Assert.Null(service.Decrypt(null, DataKey(), HmacKey(), Algorithm));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestTamperedTypeMarkerIsRejected()
    {
        var service = new EncryptionService();
        byte[] stored = service.Encrypt(1234567, DataKey(), HmacKey(), Algorithm)!;

        // The marker sits outside the AES-GCM ciphertext, so GCM alone would not notice this. The HMAC
        // is what makes it detectable - this is the reason the format carries a second key at all.
        stored[32] = (byte)TypeMarker.String;

        Assert.Throws<EncryptionException>(() => service.Decrypt(stored, DataKey(), HmacKey(), Algorithm));
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(32)] // type marker
    [InlineData(40)] // nonce
    [InlineData(50)] // ciphertext
    [InlineData(60)] // GCM tag
    public void TestTamperedByteAnywhereInTheSignedRegionIsRejected(int index)
    {
        var service = new EncryptionService();
        byte[] stored = service.Encrypt("123-45-6789", DataKey(), HmacKey(), Algorithm)!;
        Assert.True(index < stored.Length);

        stored[index] ^= 0xFF;

        Assert.Throws<EncryptionException>(() => service.Decrypt(stored, DataKey(), HmacKey(), Algorithm));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestWrongHmacKeyIsRejected()
    {
        var service = new EncryptionService();
        byte[] stored = service.Encrypt("secret", DataKey(), HmacKey(), Algorithm)!;

        byte[] otherHmacKey = HmacKey();
        otherHmacKey[0] ^= 0xFF;

        Assert.Throws<EncryptionException>(() => service.Decrypt(stored, DataKey(), otherHmacKey, Algorithm));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestWrongDataKeyIsRejected()
    {
        var service = new EncryptionService();
        byte[] stored = service.Encrypt("secret", DataKey(), HmacKey(), Algorithm)!;

        byte[] otherDataKey = DataKey();
        otherDataKey[0] ^= 0xFF;

        // The HMAC still passes here, because it is keyed independently of the data key. AES-GCM is what
        // catches this, and the failure must surface as EncryptionException rather than a raw
        // cryptographic exception.
        Assert.Throws<EncryptionException>(() => service.Decrypt(stored, otherDataKey, HmacKey(), Algorithm));
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(0)]
    [InlineData(32)]
    [InlineData(60)]
    public void TestTruncatedValueIsRejected(int length)
    {
        var service = new EncryptionService();
        Assert.Throws<EncryptionException>(
            () => service.Decrypt(new byte[length], DataKey(), HmacKey(), Algorithm));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestWrongDataKeyLengthIsRejected()
    {
        var service = new EncryptionService();
        Assert.Throws<EncryptionException>(
            () => service.Encrypt("x", new byte[16], HmacKey(), Algorithm));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestAes128UsesAShorterDataKey()
    {
        var service = new EncryptionService();
        byte[] key128 = Enumerable.Range(0, 16).Select(i => (byte)i).ToArray();

        byte[] stored = service.Encrypt("x", key128, HmacKey(), EncryptionAlgorithmNames.Aes128Gcm)!;
        Assert.Equal("x", service.Decrypt(stored, key128, HmacKey(), EncryptionAlgorithmNames.Aes128Gcm));

        Assert.Throws<EncryptionException>(
            () => service.Encrypt("x", DataKey(), HmacKey(), EncryptionAlgorithmNames.Aes128Gcm));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestUnsupportedAlgorithmIsRejected()
    {
        var service = new EncryptionService();
        Assert.Throws<EncryptionException>(
            () => service.Encrypt("x", DataKey(), HmacKey(), "AES-256-CBC"));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestCallerSuppliedByteArrayIsNotScrubbed()
    {
        var service = new EncryptionService();
        byte[] value = { 1, 2, 3, 4 };

        service.Encrypt(value, DataKey(), HmacKey(), Algorithm);

        // A byte[] value is serialized by reference, so scrubbing the working buffer would destroy the
        // caller's own data.
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, value);
    }
}
