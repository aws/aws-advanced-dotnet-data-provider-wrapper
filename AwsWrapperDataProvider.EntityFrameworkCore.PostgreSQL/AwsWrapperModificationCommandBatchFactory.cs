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

using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Update;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure.Internal;

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL;

/// <summary>
///     Factory for creating <see cref="AwsWrapperModificationCommandBatch"/> instances
///     that support wrapper data readers.
/// </summary>
public class AwsWrapperModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    private const int DefaultMaxBatchSize = 1000;

    private readonly ModificationCommandBatchFactoryDependencies dependencies;
    private readonly int maxBatchSize;

    public AwsWrapperModificationCommandBatchFactory(
        ModificationCommandBatchFactoryDependencies dependencies,
        IDbContextOptions options)
    {
        this.dependencies = dependencies;

#pragma warning disable EF1001 // Internal EF Core API usage.
        this.maxBatchSize = options.FindExtension<NpgsqlOptionsExtension>()?.MaxBatchSize
            ?? options.FindExtension<AwsWrapperOptionsExtension>()?.MaxBatchSize
            ?? DefaultMaxBatchSize;
#pragma warning restore EF1001 // Internal EF Core API usage.

        if (this.maxBatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(RelationalOptionsExtension.MaxBatchSize),
                RelationalStrings.InvalidMaxBatchSize(this.maxBatchSize));
        }
    }

    public virtual ModificationCommandBatch Create()
        => new AwsWrapperModificationCommandBatch(this.dependencies, this.maxBatchSize);
}
