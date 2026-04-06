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
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL;

public class AwsWrapperOptionsExtension : RelationalOptionsExtension, IDbContextOptionsExtension
{
    private AwsWrapperDbContextOptionsExtensionInfo? info;

    // The full wrapper connection string (may contain Plugins=... and other wrapper-specific properties).
    // This is kept separate from the base RelationalOptionsExtension.ConnectionString,
    // which should only contain properties understood by the underlying provider (Npgsql).
    public string? WrapperConnectionString { get; set; }

    // Add more AWS Wrapper settings here if needed

    public AwsWrapperOptionsExtension(IDbContextOptionsExtension wrappedExtension)
    {
        this.WrappedExtension = wrappedExtension;
    }

    protected internal AwsWrapperOptionsExtension(AwsWrapperOptionsExtension copyFrom) : base(copyFrom)
    {
        this.WrappedExtension = copyFrom.WrappedExtension;
        this.WrapperConnectionString = copyFrom.WrapperConnectionString;
    }

    public override void ApplyServices(IServiceCollection services)
    {
        this.WrappedExtension.ApplyServices(services);

        var targetRelationalConnectionServiceDescriptor = services
            .FirstOrDefault(x => x.ServiceType == typeof(IRelationalConnection));

        Type? targetRelationalConnectionType = targetRelationalConnectionServiceDescriptor?.ImplementationType;
        Func<IServiceProvider, object>? targetRelationalConnectionImplementationFactory = targetRelationalConnectionServiceDescriptor?.ImplementationFactory;

        if (targetRelationalConnectionType != null)
        {
            services.Add(new ServiceDescriptor(
                typeof(IAwsWrapperRelationalConnection),
                p => new AwsWrapperRelationalConnection(
                    p.GetRequiredService<RelationalConnectionDependencies>(),
                    (IRelationalConnection)p.GetRequiredService(targetRelationalConnectionType)),
                ServiceLifetime.Scoped));
        }
        else if (targetRelationalConnectionImplementationFactory != null)
        {
            services.Add(new ServiceDescriptor(
                typeof(IAwsWrapperRelationalConnection),
                p => new AwsWrapperRelationalConnection(
                    p.GetRequiredService<RelationalConnectionDependencies>(),
                    (IRelationalConnection)targetRelationalConnectionImplementationFactory.Invoke(p)),
                ServiceLifetime.Scoped));
        }
        else
        {
            throw new InvalidOperationException("Could not determine the target relational connection type or factory.");
        }

        services.Replace(new ServiceDescriptor(
            typeof(IRelationalConnection),
            p => p.GetRequiredService<IAwsWrapperRelationalConnection>(),
            ServiceLifetime.Scoped));

        // Replace the modification command batch factory with our wrapper-aware version
        // to handle the NpgsqlDataReader cast issue when using wrapper connections.
        services.Replace(new ServiceDescriptor(
            typeof(IModificationCommandBatchFactory),
            typeof(AwsWrapperModificationCommandBatchFactory),
            ServiceLifetime.Scoped));
    }

    protected override RelationalOptionsExtension Clone() => new AwsWrapperOptionsExtension(this);

    /// <inheritdoc />
    public override DbContextOptionsExtensionInfo Info => this.info ??= new AwsWrapperDbContextOptionsExtensionInfo(this);

    public IDbContextOptionsExtension WrappedExtension { get; set; }
}
