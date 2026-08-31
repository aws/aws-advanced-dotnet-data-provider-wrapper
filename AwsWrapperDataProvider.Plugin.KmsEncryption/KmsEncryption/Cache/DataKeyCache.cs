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

using System.Collections.Concurrent;
using System.Security.Cryptography;
using AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Model;
using Microsoft.Extensions.Caching.Memory;

namespace AwsWrapperDataProvider.Plugin.KmsEncryption.KmsEncryption.Cache;

/// <summary>
/// Caches plaintext data keys so that the common encrypt/decrypt path does not call AWS KMS.
/// <para>
/// <b>Ownership.</b> The cache owns the array it stores, and callers always receive a private copy.
/// Handing out the stored array instead would let an eviction (expiry, capacity, replacement, removal,
/// or disposal) overwrite key bytes that a caller is still using, which would silently encrypt or
/// decrypt under an all-zero key rather than fail. A data key is 32 bytes, so copying is negligible
/// next to the AES operation it feeds. Callers own their copy and may zero it when done.
/// </para>
/// <para>
/// <b>Shared lookups.</b> Concurrent misses for the same key share one KMS call, coordinated through
/// <see cref="pendingLookups"/>. <see cref="ConcurrentDictionary{TKey, TValue}.GetOrAdd(TKey, Func{TKey, TValue})"/>
/// returns the instance that is actually stored, so every racing caller observes the same
/// <see cref="Lazy{T}"/>; because <see cref="Lazy{T}"/> defers the factory until its value is read, a
/// losing caller's factory never starts. This coordination cannot be done with the cache entry itself:
/// <c>IMemoryCache.GetOrCreate</c> returns the calling thread's own value rather than the stored one, so
/// racing callers would each run their own factory and each issue a separate billable KMS request.
/// </para>
/// <para>
/// <b>Scrubbing.</b> Plaintext data keys are secret material, so a stored key is overwritten with
/// <see cref="CryptographicOperations.ZeroMemory"/> once it leaves the cache. <see cref="Dispose"/>
/// scrubs synchronously before returning. Eviction-driven scrubbing is best effort and can lag well
/// behind expiry: the cache dispatches post-eviction callbacks to the thread pool, and it only notices
/// an expired entry when it is next touched or when its expiration scan runs - once a minute by
/// default - so an expired key can stay readable for roughly that long. Note this only scrubs this
/// class's copy: callers hold their own copies, and the AWS SDK surfaces KMS plaintext through its own
/// buffers, so it is defence in depth rather than a guarantee that no copy of the key remains in the
/// heap.
/// </para>
/// </summary>
internal sealed class DataKeyCache : IDisposable
{
    private readonly MemoryCache cache;

    /// <summary>
    /// The keys this cache currently owns, so that <see cref="Dispose"/> can scrub them synchronously
    /// rather than depending on thread-pool eviction callbacks that may never run before process exit.
    /// </summary>
    private readonly ConcurrentDictionary<string, byte[]> owned = new();

    /// <summary>
    /// KMS lookups that are currently running, keyed by cache key, so that callers asking for the same
    /// key at the same time wait on one lookup instead of starting their own. See the shared-lookup
    /// remarks on the class.
    /// </summary>
    private readonly ConcurrentDictionary<string, Lazy<Task<byte[]>>> pendingLookups = new();

    private readonly TimeSpan expiration;
    private readonly bool enabled;

    internal DataKeyCache(EncryptionConfig config)
    {
        this.enabled = config.DataKeyCacheEnabled;
        this.expiration = config.DataKeyCacheExpiration;

        // SizeLimit is expressed in the same unit as the per-entry Size set in GetOrAddAsync, so one
        // unit is one data key.
        this.cache = new MemoryCache(new MemoryCacheOptions
        {
            SizeLimit = config.DataKeyCacheMaxSize,
        });
    }

    /// <summary>
    /// Returns the plaintext data key for <paramref name="cacheKey"/>, invoking
    /// <paramref name="dataKeyFactory"/> only when the key is not already cached. When caching is
    /// disabled the factory is invoked on every call and nothing is retained.
    /// </summary>
    /// <param name="cacheKey">Identifies the data key; typically the key id from the metadata.</param>
    /// <param name="dataKeyFactory">Retrieves the plaintext data key from AWS KMS.</param>
    /// <returns>
    /// A private copy of the plaintext data key, owned by the caller. Never the cache's own array.
    /// </returns>
    internal async Task<byte[]> GetOrAddAsync(string cacheKey, Func<Task<byte[]>> dataKeyFactory)
    {
        if (!this.enabled)
        {
            // Nothing is retained, so the caller owns the factory's array outright.
            return await dataKeyFactory().ConfigureAwait(false);
        }

        if (this.cache.TryGetValue(cacheKey, out byte[]? cached) && cached is not null)
        {
            return (byte[])cached.Clone();
        }

        Lazy<Task<byte[]>> pending = this.pendingLookups.GetOrAdd(
            cacheKey,
            key => new Lazy<Task<byte[]>>(
                async () =>
                {
                    // The entry may have been published by a lookup that finished while this caller was
                    // between its own miss above and this GetOrAdd, in which case there is nothing to
                    // fetch.
                    if (this.cache.TryGetValue(key, out byte[]? raced) && raced is not null)
                    {
                        return (byte[])raced.Clone();
                    }

                    byte[] fromKms = await dataKeyFactory().ConfigureAwait(false);
                    this.Publish(key, fromKms);
                    return fromKms;
                },
                LazyThreadSafetyMode.ExecutionAndPublication));

        byte[] shared;
        try
        {
            shared = await pending.Value.ConfigureAwait(false);
        }
        finally
        {
            // Dropping the pending entry unconditionally means a failure is never replayed to later
            // callers: the next miss starts a fresh KMS call instead of re-awaiting a faulted task.
            this.pendingLookups.TryRemove(new KeyValuePair<string, Lazy<Task<byte[]>>>(cacheKey, pending));
        }

        // The shared result is never the array the cache stores, so no eviction callback can reach it.
        // Every caller gets its own copy of it.
        return (byte[])shared.Clone();
    }

    /// <summary>
    /// Publishes a copy of <paramref name="fetched"/> that the cache alone references.
    /// <para>
    /// This runs inside the shared lookup so that it happens exactly once however many callers
    /// coalesced. Publishing once per caller would instead replace an entry whose value is the
    /// identical array, and the resulting <see cref="EvictionReason.Replaced"/> callback would scrub
    /// the very array the cache continues to serve - leaving an all-zero key in the cache for the rest
    /// of its lifetime. Because nothing outside the cache references the stored array, every eviction
    /// reason is safe to scrub unconditionally.
    /// </para>
    /// </summary>
    private void Publish(string cacheKey, byte[] fetched)
    {
        byte[] forCache = (byte[])fetched.Clone();

        this.owned[cacheKey] = forCache;
        this.cache.Set(cacheKey, forCache, new MemoryCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = this.expiration,
            Size = 1,
            PostEvictionCallbacks =
            {
                new PostEvictionCallbackRegistration { EvictionCallback = this.ZeroEvictedKey },
            },
        });
    }

    /// <summary>
    /// Gets the arrays this cache currently owns. Exists so that tests can assert the scrubbing
    /// guarantee, which is otherwise unobservable: the stored arrays are copies that no caller ever
    /// receives.
    /// </summary>
    internal IReadOnlyCollection<byte[]> OwnedKeysForTest => this.owned.Values.ToArray();

    public void Dispose()
    {
        // Scrub synchronously from this class's own registry. This is load-bearing rather than a
        // belt-and-braces measure: disposing the cache fires no post-eviction callbacks at all, so
        // without this loop nothing would ever be scrubbed on the disposal path and the process could
        // exit with every cached plaintext key still readable in the heap.
        foreach (KeyValuePair<string, byte[]> entry in this.owned)
        {
            if (this.owned.TryRemove(entry))
            {
                CryptographicOperations.ZeroMemory(entry.Value);
            }
        }

        this.cache.Dispose();
    }

    /// <summary>
    /// Scrubs a key that has left the cache. Safe against a stale callback: the entry is only dropped
    /// from <see cref="owned"/> when it still maps to this exact array, so a replacement key that
    /// happens to share the cache key is left alone.
    /// </summary>
    private void ZeroEvictedKey(object key, object? value, EvictionReason reason, object? state)
    {
        if (value is not byte[] evicted)
        {
            return;
        }

        if (key is string cacheKey)
        {
            this.owned.TryRemove(new KeyValuePair<string, byte[]>(cacheKey, evicted));
        }

        CryptographicOperations.ZeroMemory(evicted);
    }
}
