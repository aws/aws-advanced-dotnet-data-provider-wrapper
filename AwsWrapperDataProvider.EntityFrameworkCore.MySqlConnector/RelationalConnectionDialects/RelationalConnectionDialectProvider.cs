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

using Microsoft.EntityFrameworkCore.Infrastructure;

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector.RelationalConnectionDialects;

/// <summary>
/// Selects <see cref="IRelationalConnectionDialect"/> for the configured EF Core MySQL provider.
/// </summary>
public static class RelationalConnectionDialectProvider
{
    private static readonly Dictionary<string, IRelationalConnectionDialect> DialectsByAssemblyPrefix = new()
    {
        { EfMySqlAssemblyPrefixes.Pomelo, PomeloEfMySqlRelationalConnectionDialect.Instance },
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

        return
            "The relational connection from the wrapped Entity Framework Core provider is not supported for AWS Advanced .NET Data Provider MySQL integration. " +
            $"Wrapped extension: {extensionDetail}. " +
            "Supported provider: Pomelo.EntityFrameworkCore.MySql.";
    }
}
