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

using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Cache;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.KmsEncryption;

public class DataKeyCacheTests
{
    private const string KeyId = "key-1";

    private static Dictionary<string, string> Props(
        bool dataKeyCacheEnabled = true,
        string dataKeyCacheExpirationMs = "3600000")
    {
        return new Dictionary<string, string>
        {
            { PropertyDefinition.KmsRegion.Name, "us-east-1" },
            { PropertyDefinition.KmsDataKeyCacheEnabled.Name, dataKeyCacheEnabled.ToString() },
            { PropertyDefinition.KmsDataKeyCacheExpirationMs.Name, dataKeyCacheExpirationMs },
        };
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestCachedKeyIsReusedWithoutCallingFactoryAgain()
    {
        using var cache = new DataKeyCache(EncryptionConfig.FromProperties(Props()));
        int factoryCalls = 0;

        Task<byte[]> Factory()
        {
            Interlocked.Increment(ref factoryCalls);
            return Task.FromResult(new byte[] { 1, 2, 3 });
        }

        byte[] first = await cache.GetOrAddAsync(KeyId, Factory);
        byte[] second = await cache.GetOrAddAsync(KeyId, Factory);

        Assert.Equal(new byte[] { 1, 2, 3 }, first);
        Assert.Equal(new byte[] { 1, 2, 3 }, second);
        Assert.Equal(1, factoryCalls);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestConcurrentMissesShareASingleFactoryCall()
    {
        const int Callers = 8;
        using var cache = new DataKeyCache(EncryptionConfig.FromProperties(Props()));
        int factoryCalls = 0;

        Task<byte[]> Factory()
        {
            Interlocked.Increment(ref factoryCalls);
            return Task.FromResult(new byte[] { 9 });
        }

        // A Barrier is what makes this test meaningful. Simply starting N tasks is not enough: the
        // first caller usually populates the cache before the rest begin, so every later caller is a
        // hit and the assertion below would hold even if no lookup were ever shared. Releasing all
        // callers from the barrier guarantees they are simultaneously past the cache lookup and
        // therefore all genuinely missing, which is the only state in which sharing is under test.
        using var barrier = new Barrier(Callers);
        Task<byte[]>[] lookups = Enumerable.Range(0, Callers)
            .Select(_ => Task.Run(() =>
            {
                barrier.SignalAndWait();
                return cache.GetOrAddAsync(KeyId, Factory);
            }))
            .ToArray();

        byte[][] results = await Task.WhenAll(lookups);

        Assert.Equal(1, factoryCalls);
        Assert.All(results, r => Assert.Equal(new byte[] { 9 }, r));

        // Each caller must own its copy; sharing one array would let an eviction scrub a live key.
        byte[][] distinct = results.Distinct(ReferenceEqualityComparer.Instance).Cast<byte[]>().ToArray();
        Assert.Equal(Callers, distinct.Length);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestCallerReceivesPrivateCopyNotTheCachedArray()
    {
        using var cache = new DataKeyCache(EncryptionConfig.FromProperties(Props()));
        byte[] fromKms = { 1, 2, 3, 4 };

        byte[] first = await cache.GetOrAddAsync(KeyId, () => Task.FromResult(fromKms));
        byte[] second = await cache.GetOrAddAsync(KeyId, () => Task.FromResult(new byte[] { 5, 5, 5, 5 }));

        Assert.NotSame(fromKms, first);
        Assert.NotSame(first, second);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, first);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, second);

        // Mutating a returned copy must not corrupt the cached key for later callers.
        first[0] = 0xFF;
        byte[] third = await cache.GetOrAddAsync(KeyId, () => Task.FromResult(new byte[] { 6, 6, 6, 6 }));
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, third);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestDisabledCacheCallsFactoryEveryTime()
    {
        using var cache = new DataKeyCache(
            EncryptionConfig.FromProperties(Props(dataKeyCacheEnabled: false)));
        int factoryCalls = 0;

        Task<byte[]> Factory()
        {
            Interlocked.Increment(ref factoryCalls);
            return Task.FromResult(new byte[] { 7 });
        }

        await cache.GetOrAddAsync(KeyId, Factory);
        await cache.GetOrAddAsync(KeyId, Factory);
        await cache.GetOrAddAsync(KeyId, Factory);

        Assert.Equal(3, factoryCalls);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestFailedLookupIsNotCached()
    {
        using var cache = new DataKeyCache(EncryptionConfig.FromProperties(Props()));
        int factoryCalls = 0;

        Task<byte[]> FailingThenSucceeding()
        {
            return Interlocked.Increment(ref factoryCalls) == 1
                ? Task.FromException<byte[]>(new InvalidOperationException("kms unavailable"))
                : Task.FromResult(new byte[] { 4 });
        }

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => cache.GetOrAddAsync(KeyId, FailingThenSucceeding));

        // A transient failure must not be replayed to later callers until the entry expires.
        byte[] recovered = await cache.GetOrAddAsync(KeyId, FailingThenSucceeding);

        Assert.Equal(new byte[] { 4 }, recovered);
        Assert.Equal(2, factoryCalls);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestDisposeZeroesCachedKeyMaterialSynchronously()
    {
        byte[] fromKms = { 1, 2, 3, 4 };
        var cache = new DataKeyCache(EncryptionConfig.FromProperties(Props()));

        byte[] returned = await cache.GetOrAddAsync(KeyId, () => Task.FromResult(fromKms));
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, returned);

        // The cache stores its own copy, which no caller ever receives, so the arrays it will scrub are
        // reachable only through this accessor.
        byte[][] cacheOwned = cache.OwnedKeysForTest.ToArray();
        byte[] single = Assert.Single(cacheOwned);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, single);
        Assert.NotSame(fromKms, single);
        Assert.NotSame(returned, single);

        cache.Dispose();

        // Asserted immediately, with no waiting or retry: Dispose scrubs from its own registry on the
        // calling thread. Depending on the cache's post-eviction callbacks here would make this racy,
        // because those are dispatched to the thread pool and can run long after Dispose returns.
        Assert.Equal(new byte[] { 0, 0, 0, 0 }, single);
        Assert.Empty(cache.OwnedKeysForTest);

        // Neither the caller's copy nor the factory's array is touched, so an in-flight cryptographic
        // operation cannot be corrupted by the cache scrubbing its own key.
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, returned);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, fromKms);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestFailedLookupDoesNotDisturbAKeyAnotherCallerObtained()
    {
        using var cache = new DataKeyCache(EncryptionConfig.FromProperties(Props()));

        // A slow failing lookup, overlapped with a successful one for the same key. The failing path
        // must not scrub or evict the good key: a caller that never saw an error would otherwise carry
        // on encrypting under an all-zero key.
        var failureGate = new TaskCompletionSource();
        Task<byte[]> slowFailing = cache.GetOrAddAsync(KeyId, async () =>
        {
            await failureGate.Task.ConfigureAwait(false);
            throw new InvalidOperationException("kms throttled");
        });

        byte[] good = await cache.GetOrAddAsync("other-key", () => Task.FromResult(new byte[] { 1, 2, 3, 4 }));

        failureGate.SetResult();
        await Assert.ThrowsAsync<InvalidOperationException>(() => slowFailing);

        Assert.Equal(new byte[] { 1, 2, 3, 4 }, good);
        byte[] stillCached = await cache.GetOrAddAsync("other-key", () => Task.FromResult(new byte[] { 9, 9, 9, 9 }));
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, stillCached);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task TestCoalescedMissesDoNotPoisonTheCachedKey()
    {
        const int Callers = 2;
        using var cache = new DataKeyCache(EncryptionConfig.FromProperties(Props()));

        // Two simultaneous misses are enough. If each coalesced caller published the key itself, the
        // second publish would replace an entry holding the identical array, and the resulting
        // Replaced callback would scrub the array the cache is still serving - leaving an all-zero key
        // cached for the rest of its lifetime while no caller saw anything go wrong.
        using var barrier = new Barrier(Callers);
        static async Task<byte[]> Kms()
        {
            // Real KMS latency is what widens the window; an already-completed task hides the defect.
            await Task.Delay(30).ConfigureAwait(false);
            return new byte[] { 9, 8, 7, 6 };
        }

        await Task.WhenAll(Enumerable.Range(0, Callers).Select(_ => Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await cache.GetOrAddAsync(KeyId, Kms);
        })));

        // A delay is legitimate here and only here: eviction callbacks are dispatched to the thread
        // pool, so a wrongly fired scrub needs time to land before the cache is read back.
        await Task.Delay(100, TestContext.Current.CancellationToken);

        byte[] later = await cache.GetOrAddAsync(
            KeyId,
            () => throw new InvalidOperationException("must be served from the cache"));

        Assert.Equal(new byte[] { 9, 8, 7, 6 }, later);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("not-a-number")]
    public void TestInvalidExpirationIsRejected(string expirationMs)
    {
        Assert.Throws<ArgumentException>(
            () => EncryptionConfig.FromProperties(Props(dataKeyCacheExpirationMs: expirationMs)));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TestMissingRegionIsRejected()
    {
        Dictionary<string, string> props = Props();
        props.Remove(PropertyDefinition.KmsRegion.Name);

        Assert.Throws<ArgumentException>(() => EncryptionConfig.FromProperties(props));
    }
}
