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

using Amazon.KeyManagementService;
using Amazon.KeyManagementService.Model;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Cache;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Key;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Service;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.KmsEncryption;

public class ColumnEncryptorTests
{
    private const string MasterKeyArn = "arn:aws:kms:us-east-1:123456789012:key/abcd";

    /// <summary>The plaintext data key AWS KMS is pretended to return.</summary>
    private static byte[] PlaintextDataKey() => Enumerable.Range(0, 32).Select(i => (byte)i).ToArray();

    private static byte[] HmacKey() => Enumerable.Range(0, 32).Select(i => (byte)(0xA0 + i)).ToArray();

    private static DataKeyMetadata Metadata(string keySpec = "AES_256") => new(
        keyId: "key-1",
        masterKeyArn: MasterKeyArn,
        encryptedDataKey: Convert.ToBase64String(new byte[] { 9, 9, 9, 9 }),
        hmacKey: HmacKey(),
        keySpec: keySpec);

    private static ColumnEncryptionConfig SsnColumn(
        DataKeyMetadata? metadata = null,
        string algorithm = "AES-256-GCM") =>
        new("users", "ssn", "key-1", algorithm, metadata ?? Metadata());

    private static Dictionary<string, string> Props() => new()
    {
        { PropertyDefinition.KmsRegion.Name, "us-east-1" },
    };

    /// <summary>Returns a KMS stand-in that hands back a fixed data key and counts calls.</summary>
    private static Mock<IAmazonKeyManagementService> KmsReturning(byte[] dataKey)
    {
        var kms = new Mock<IAmazonKeyManagementService>();
        kms.Setup(k => k.DecryptAsync(It.IsAny<DecryptRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new DecryptResponse { Plaintext = new MemoryStream(dataKey) });
        return kms;
    }

    private static (ColumnEncryptor Encryptor, KeyManager Manager) Build(Mock<IAmazonKeyManagementService> kms)
    {
        var cache = new DataKeyCache(EncryptionConfig.FromProperties(Props()));
        var manager = new KeyManager(kms.Object, cache);
        return (new ColumnEncryptor(manager, new EncryptionService()), manager);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestValueSurvivesARoundTripThroughTheColumnEncryptor()
    {
        Mock<IAmazonKeyManagementService> kms = KmsReturning(PlaintextDataKey());
        (ColumnEncryptor encryptor, KeyManager manager) = Build(kms);
        using (manager)
        {
            ColumnEncryptionConfig column = SsnColumn();

            byte[] stored = await encryptor.EncryptAsync("123-45-6789", column, TestContext.Current.CancellationToken);
            object? recovered = await encryptor.DecryptAsync(stored, column, TestContext.Current.CancellationToken);

            Assert.Equal("123-45-6789", recovered);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestDataKeyIsFetchedFromKmsOnceAcrossManyValues()
    {
        Mock<IAmazonKeyManagementService> kms = KmsReturning(PlaintextDataKey());
        (ColumnEncryptor encryptor, KeyManager manager) = Build(kms);
        using (manager)
        {
            ColumnEncryptionConfig column = SsnColumn();
            for (int i = 0; i < 50; i++)
            {
                await encryptor.EncryptAsync($"value-{i}", column, TestContext.Current.CancellationToken);
            }

            // Encrypting a column of 50 rows must not mean 50 billable KMS calls.
            kms.Verify(
                k => k.DecryptAsync(It.IsAny<DecryptRequest>(), It.IsAny<CancellationToken>()),
                Times.Once);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestRotatingTheDataKeyProducesAFreshKmsCall()
    {
        Mock<IAmazonKeyManagementService> kms = KmsReturning(PlaintextDataKey());
        (ColumnEncryptor encryptor, KeyManager manager) = Build(kms);
        using (manager)
        {
            await encryptor.EncryptAsync("a", SsnColumn(), TestContext.Current.CancellationToken);

            // A rotated key has a different encrypted data key, which is what the cache is keyed on. Keying
            // on the key id instead would silently keep using the superseded plaintext.
            var rotated = new DataKeyMetadata(
                keyId: "key-1",
                masterKeyArn: MasterKeyArn,
                encryptedDataKey: Convert.ToBase64String(new byte[] { 7, 7, 7, 7 }),
                hmacKey: HmacKey(),
                keySpec: "AES_256");
            await encryptor.EncryptAsync("b", SsnColumn(rotated), TestContext.Current.CancellationToken);

            kms.Verify(
                k => k.DecryptAsync(It.IsAny<DecryptRequest>(), It.IsAny<CancellationToken>()),
                Times.Exactly(2));
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestMissingKeyMaterialIsRefused()
    {
        Mock<IAmazonKeyManagementService> kms = KmsReturning(PlaintextDataKey());
        (ColumnEncryptor encryptor, KeyManager manager) = Build(kms);
        using (manager)
        {
            // The metadata names a key that is absent from the key storage table. Encryption must fail
            // rather than fall back, or the column would be left readable.
            ColumnEncryptionConfig broken = new("users", "ssn", "key-missing", "AES-256-GCM");

            EncryptionException ex = await Assert.ThrowsAsync<EncryptionException>(
                () => encryptor.EncryptAsync("123-45-6789", broken, TestContext.Current.CancellationToken));
            Assert.Contains("key-missing", ex.Message);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestKmsFailureSurfacesAsEncryptionExceptionWithoutLeakingDetail()
    {
        var kms = new Mock<IAmazonKeyManagementService>();
        kms.Setup(k => k.DecryptAsync(It.IsAny<DecryptRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonKeyManagementServiceException("access denied"));
        (ColumnEncryptor encryptor, KeyManager manager) = Build(kms);
        using (manager)
        {
            EncryptionException ex = await Assert.ThrowsAsync<EncryptionException>(
                () => encryptor.EncryptAsync("123-45-6789", SsnColumn(), TestContext.Current.CancellationToken));

            Assert.Contains("kms:Decrypt", ex.Message);
            Assert.IsType<AmazonKeyManagementServiceException>(ex.InnerException);

            // The message must never carry the value being encrypted.
            Assert.DoesNotContain("123-45-6789", ex.Message);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestDataKeyOfTheWrongLengthIsRefused()
    {
        // A 16-byte key returned for an AES_256 column means the stored metadata and the stored key
        // disagree; using it anyway would encrypt under AES-128 while the metadata claims AES-256.
        Mock<IAmazonKeyManagementService> kms = KmsReturning(new byte[16]);
        (ColumnEncryptor encryptor, KeyManager manager) = Build(kms);
        using (manager)
        {
            EncryptionException ex = await Assert.ThrowsAsync<EncryptionException>(
                () => encryptor.EncryptAsync("123-45-6789", SsnColumn(), TestContext.Current.CancellationToken));
            Assert.Contains("AES_256", ex.Message);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestUnsupportedKeySpecIsRefused()
    {
        Mock<IAmazonKeyManagementService> kms = KmsReturning(PlaintextDataKey());
        (ColumnEncryptor encryptor, KeyManager manager) = Build(kms);
        using (manager)
        {
            await Assert.ThrowsAsync<EncryptionException>(
                () => encryptor.EncryptAsync(
                    "x", SsnColumn(Metadata(keySpec: "RSA_2048")), TestContext.Current.CancellationToken));
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestMalformedStoredDataKeyIsRefused()
    {
        Mock<IAmazonKeyManagementService> kms = KmsReturning(PlaintextDataKey());
        (ColumnEncryptor encryptor, KeyManager manager) = Build(kms);
        using (manager)
        {
            var bad = new DataKeyMetadata("key-1", MasterKeyArn, "not-base64!!", HmacKey(), "AES_256");

            EncryptionException ex = await Assert.ThrowsAsync<EncryptionException>(
                () => encryptor.EncryptAsync("x", SsnColumn(bad), TestContext.Current.CancellationToken));
            Assert.Contains("base64", ex.Message);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestDecryptingWithAnotherColumnsKeyFails()
    {
        Mock<IAmazonKeyManagementService> kms = KmsReturning(PlaintextDataKey());
        (ColumnEncryptor encryptor, KeyManager manager) = Build(kms);
        using (manager)
        {
            byte[] stored = await encryptor.EncryptAsync(
                "123-45-6789", SsnColumn(), TestContext.Current.CancellationToken);

            // Each column has its own HMAC key, so a value encrypted for one column must not be readable as
            // another. This is why a value cannot be re-labelled for a different column after encryption.
            byte[] otherHmac = HmacKey();
            otherHmac[0] ^= 0xFF;
            var otherKey = new DataKeyMetadata(
                "key-2", MasterKeyArn, Convert.ToBase64String(new byte[] { 9, 9, 9, 9 }), otherHmac, "AES_256");
            var otherColumn = new ColumnEncryptionConfig("audit", "note", "key-2", "AES-256-GCM", otherKey);

            await Assert.ThrowsAsync<EncryptionException>(
                () => encryptor.DecryptAsync(stored, otherColumn, TestContext.Current.CancellationToken));
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestShortPlaintextInAnEncryptedColumnIsReportedAsNotEncrypted()
    {
        Mock<IAmazonKeyManagementService> kms = KmsReturning(PlaintextDataKey());
        (ColumnEncryptor encryptor, KeyManager manager) = Build(kms);
        using (manager)
        {
            // What a row looks like when it was written without the plugin, as a literal, or by another
            // tool. Any encrypted value is at least 61 bytes, so this is conclusively not one.
            byte[] plaintextInColumn = System.Text.Encoding.UTF8.GetBytes("123-45-6789");

            EncryptionException ex = await Assert.ThrowsAsync<EncryptionException>(
                () => encryptor.DecryptAsync(plaintextInColumn, SsnColumn(), TestContext.Current.CancellationToken));

            Assert.Contains("users.ssn", ex.Message);
            Assert.Contains("is not encrypted", ex.Message);
            Assert.Contains("literal", ex.Message);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestLongPlaintextInAnEncryptedColumnFailsTheIntegrityCheck()
    {
        Mock<IAmazonKeyManagementService> kms = KmsReturning(PlaintextDataKey());
        (ColumnEncryptor encryptor, KeyManager manager) = Build(kms);
        using (manager)
        {
            // Long enough to pass the length check, so the integrity check is what catches it. The message
            // has to name "not encrypted" as a cause, because that is more likely in practice than
            // tampering.
            byte[] longPlaintext = System.Text.Encoding.UTF8.GetBytes(new string('x', 200));

            EncryptionException ex = await Assert.ThrowsAsync<EncryptionException>(
                () => encryptor.DecryptAsync(longPlaintext, SsnColumn(), TestContext.Current.CancellationToken));

            Assert.Contains("users.ssn", ex.Message);
            Assert.Contains("not encrypted", ex.Message);
            Assert.Contains("different key", ex.Message);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestNullStoredValueDecryptsToNull()
    {
        Mock<IAmazonKeyManagementService> kms = KmsReturning(PlaintextDataKey());
        (ColumnEncryptor encryptor, KeyManager manager) = Build(kms);
        using (manager)
        {
            Assert.Null(await encryptor.DecryptAsync(null, SsnColumn(), TestContext.Current.CancellationToken));
        }
    }
}
