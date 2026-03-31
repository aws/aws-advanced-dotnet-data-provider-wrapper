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
    private const string PomeloEntityFrameworkCoreMySqlAssemblyPrefix = "Pomelo.EntityFrameworkCore.MySql";

    /// <summary>
    /// Detects which EF MySQL provider registered <paramref name="wrappedExtension"/>.
    /// </summary>
    /// <param name="wrappedExtension">The wrapped options extension (e.g. from <c>UseMySql</c>).</param>
    /// <returns>The detected provider kind.</returns>
    public static EfMySqlProviderKind DetectEfMySqlProviderKind(IDbContextOptionsExtension? wrappedExtension)
    {
        if (wrappedExtension is null)
        {
            return EfMySqlProviderKind.Unknown;
        }

        var assemblyName = wrappedExtension.GetType().Assembly.GetName().Name ?? string.Empty;
        if (assemblyName.StartsWith(PomeloEntityFrameworkCoreMySqlAssemblyPrefix, StringComparison.Ordinal))
        {
            return EfMySqlProviderKind.Pomelo;
        }

        return EfMySqlProviderKind.Unknown;
    }

    /// <summary>
    /// Returns the dialect for <paramref name="providerKind"/>.
    /// </summary>
    /// <param name="providerKind">The EF MySQL provider kind.</param>
    /// <param name="wrappedExtensionForDiagnostics">The wrapped extension (included in exception detail when kind is <see cref="EfMySqlProviderKind.Unknown"/>).</param>
    /// <returns>The dialect instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="providerKind"/> is <see cref="EfMySqlProviderKind.Unknown"/>.</exception>
    public static IRelationalConnectionDialect GetDialect(
        EfMySqlProviderKind providerKind,
        IDbContextOptionsExtension? wrappedExtensionForDiagnostics = null)
    {
        if (providerKind == EfMySqlProviderKind.Unknown)
        {
            throw new InvalidOperationException(BuildUnsupportedRelationalConnectionMessage(providerKind, wrappedExtensionForDiagnostics));
        }

        return providerKind switch
        {
            EfMySqlProviderKind.Pomelo => PomeloEfMySqlRelationalConnectionDialect.Instance,
            _ => throw new InvalidOperationException(BuildUnsupportedRelationalConnectionMessage(providerKind, wrappedExtensionForDiagnostics)),
        };
    }

    private static string BuildUnsupportedRelationalConnectionMessage(
        EfMySqlProviderKind providerKind,
        IDbContextOptionsExtension? wrappedExtension)
    {
        var extensionDetail = wrappedExtension is null
            ? "none (wrapped extension was null)"
            : $"{wrappedExtension.GetType().FullName} (Assembly={wrappedExtension.GetType().Assembly.GetName().Name})";

        return
            "The relational connection from the wrapped Entity Framework Core provider is not supported for AWS Advanced .NET Data Provider MySQL integration " +
            $"({nameof(EfMySqlProviderKind)}={providerKind}). " +
            $"Wrapped extension: {extensionDetail}. " +
            "Supported provider: Pomelo.EntityFrameworkCore.MySql.";
    }
}
