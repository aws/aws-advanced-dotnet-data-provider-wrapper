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
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector.RelationalConnectionDialects;

/// <summary>
/// Selects <see cref="IRelationalConnectionDialect"/> for the configured EF Core MySQL provider.
/// </summary>
public static class RelationalConnectionDialectProvider
{
    // Concurrent because RegisterDialect is public: a caller registering a dialect on one thread would
    // otherwise corrupt the dictionary, or throw "collection was modified" in a GetDialect enumerating
    // it on another. The dialect is shared rather than provider-specific: the mandatory options it
    // applies follow from Entity Framework Core's use of MySqlConnector, not from any one provider.
    private static readonly ConcurrentDictionary<string, IRelationalConnectionDialect> DialectsByAssemblyPrefix = new()
    {
        [EfMySqlAssemblyPrefixes.Microting] = EfMySqlRelationalConnectionDialect.Instance,
    };

    /// <summary>
    /// Returns the dialect for the EF MySQL provider that registered <paramref name="wrappedExtension"/>.
    /// </summary>
    /// <param name="wrappedExtension">The wrapped options extension (e.g. from <c>UseMySql</c>).</param>
    /// <returns>The dialect instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no supported provider is detected.</exception>
    public static IRelationalConnectionDialect GetDialect(IDbContextOptionsExtension? wrappedExtension)
    {
        if (wrappedExtension is not null)
        {
            var assemblyName = wrappedExtension.GetType().Assembly.GetName().Name ?? string.Empty;
            foreach (var (prefix, dialect) in DialectsByAssemblyPrefix)
            {
                if (assemblyName.StartsWith(prefix, StringComparison.Ordinal))
                {
                    return dialect;
                }
            }
        }

        throw new InvalidOperationException(BuildUnsupportedRelationalConnectionMessage(wrappedExtension));
    }

    /// <summary>
    /// Registers a dialect for an EF Core MySQL provider that is not supported out of the box,
    /// replacing any dialect already registered for the same prefix. Safe to call concurrently.
    /// </summary>
    /// <param name="assemblyPrefix">Assembly name prefix of the provider's options extension.</param>
    /// <param name="dialect">The dialect to use for that provider.</param>
    public static void RegisterDialect(string assemblyPrefix, IRelationalConnectionDialect dialect)
    {
        DialectsByAssemblyPrefix[assemblyPrefix] = dialect;
    }

    private static string BuildUnsupportedRelationalConnectionMessage(
        IDbContextOptionsExtension? wrappedExtension)
    {
        var extensionDetail = wrappedExtension is null
            ? "none (wrapped extension was null)"
            : $"{wrappedExtension.GetType().FullName} (Assembly={wrappedExtension.GetType().Assembly.GetName().Name})";

        // The supported provider is named from the registered prefixes rather than hard-coded, so a
        // dialect added through RegisterDialect is listed here too instead of being omitted.
        var supportedProviders = string.Join(", ", DialectsByAssemblyPrefix.Keys);

        return
            "The relational connection from the wrapped Entity Framework Core provider is not supported for AWS Advanced .NET Data Provider MySQL integration. " +
            $"Wrapped extension: {extensionDetail}. " +
            $"Supported provider(s): {supportedProviders}. " +
            "Note that upstream Pomelo.EntityFrameworkCore.MySql is not supported because it has no Entity Framework Core 10 release; " +
            "Microting.EntityFrameworkCore.MySql is a fork of it built against EF Core 10. " +
            "To use a different provider, register a dialect for it with RelationalConnectionDialectProvider.RegisterDialect.";
    }
}
