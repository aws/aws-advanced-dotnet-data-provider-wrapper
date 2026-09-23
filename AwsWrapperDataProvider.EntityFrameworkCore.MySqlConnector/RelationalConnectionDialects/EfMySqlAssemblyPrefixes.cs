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

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector.RelationalConnectionDialects;

/// <summary>
/// Assembly name prefixes of the Entity Framework Core MySQL providers that
/// <see cref="RelationalConnectionDialectProvider"/> recognizes out of the box.
/// </summary>
public static class EfMySqlAssemblyPrefixes
{
    /// <summary>
    /// Assembly name prefix of <c>Microting.EntityFrameworkCore.MySql</c>, the supported EF Core MySQL provider.
    /// </summary>
    /// <remarks>
    /// Microting is a fork of <c>Pomelo.EntityFrameworkCore.MySql</c> and is required because upstream Pomelo
    /// has no Entity Framework Core 10 release. The fork renamed its assembly and namespaces from
    /// <c>Pomelo.*</c> to <c>Microting.*</c>, and the dialect is selected by matching this assembly name, so
    /// referencing upstream Pomelo instead does not resolve a dialect.
    /// </remarks>
    public static readonly string Microting = "Microting.EntityFrameworkCore.MySql";
}
