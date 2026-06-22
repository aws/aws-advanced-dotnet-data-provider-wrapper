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

namespace AwsWrapperDataProvider.Driver.Auth;

/// <summary>
/// Process-wide registry mapping an endpoint key to the <see cref="PasswordProviderRegistration"/>
/// for that endpoint.
/// <para>
/// Token-based auth plugins register a provider keyed by their endpoint cache key and pass that key
/// through the connection properties under <see cref="ProviderKeyPropertyName"/>. Target dialects
/// look the provider up at connection-creation time and wire it into the driver's native
/// password-provider mechanism. The password itself never reaches the connection string — keeping
/// the driver's pool key stable across token rotations.
/// </para>
/// <para>
/// Entries are process-lifetime and bounded by the number of distinct endpoints (the same key space
/// as the auth plugins' token/secret caches); re-registering an endpoint overwrites the previous
/// registration.
/// </para>
/// </summary>
public static class PasswordProviderRegistry
{
    /// <summary>
    /// The reserved connection-property key under which an auth plugin stores the endpoint key that
    /// correlates a connection to its registered provider. The <c>__</c> prefix marks it as an
    /// internal runtime handle: it is stripped from user-supplied connection strings when they are
    /// parsed and is never emitted into a target connection string, so it cannot be set by users.
    /// </summary>
    public const string ProviderKeyPropertyName = "__awsWrapperPasswordProviderKey";

    private static readonly ConcurrentDictionary<string, PasswordProviderRegistration> Providers = new();

    /// <summary>
    /// Registers (or overwrites) the password provider for the given endpoint key.
    /// </summary>
    /// <param name="key">The endpoint key (typically the auth plugin's token/secret cache key).</param>
    /// <param name="registration">The provider and its refresh cadence.</param>
    public static void Register(string key, PasswordProviderRegistration registration)
    {
        Providers[key] = registration;
    }

    /// <summary>
    /// Attempts to retrieve the password provider registered for the given endpoint key.
    /// </summary>
    /// <param name="key">The endpoint key.</param>
    /// <param name="registration">The registration if found; otherwise <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if a registration was found; otherwise <see langword="false"/>.</returns>
    public static bool TryGet(string key, out PasswordProviderRegistration? registration)
    {
        return Providers.TryGetValue(key, out registration);
    }

    /// <summary>
    /// Removes the password provider registered for the given endpoint key, if any.
    /// </summary>
    /// <param name="key">The endpoint key.</param>
    public static void Remove(string key)
    {
        Providers.TryRemove(key, out _);
    }

    /// <summary>
    /// Removes all registrations. Intended for test isolation.
    /// </summary>
    internal static void Clear()
    {
        Providers.Clear();
    }
}
