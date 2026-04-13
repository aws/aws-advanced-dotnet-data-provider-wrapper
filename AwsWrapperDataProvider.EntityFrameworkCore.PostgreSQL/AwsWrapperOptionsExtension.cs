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

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL;

public class AwsWrapperOptionsExtension : IDbContextOptionsExtension
{
    private AwsWrapperDbContextOptionsExtensionInfo? info;

    public IDbContextOptionsExtension WrappedExtension { get; set; }

    public string? WrapperConnectionString { get; set; }

    public AwsWrapperOptionsExtension(IDbContextOptionsExtension wrappedExtension)
    {
        this.WrappedExtension = wrappedExtension;
    }

    public void ApplyServices(IServiceCollection services)
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

        services.Replace(new ServiceDescriptor(
            typeof(IModificationCommandBatchFactory),
            typeof(AwsWrapperModificationCommandBatchFactory),
            ServiceLifetime.Scoped));
    }

    public void Validate(IDbContextOptions options)
    {
    }

    public DbContextOptionsExtensionInfo Info => this.info ??= new AwsWrapperDbContextOptionsExtensionInfo(this);

    private sealed class AwsWrapperDbContextOptionsExtensionInfo : DbContextOptionsExtensionInfo
    {
        public AwsWrapperDbContextOptionsExtensionInfo(AwsWrapperOptionsExtension optionsExtension)
            : base(optionsExtension)
        {
        }

        public override bool IsDatabaseProvider => false;

        public override string LogFragment => $"Using AWS Wrapper Provider - ConnectionString: {this.ConnectionString}";

        public override int GetServiceProviderHashCode() => (this.ConnectionString ?? string.Empty).GetHashCode();

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            debugInfo["AwsWrapper:" + nameof(AwsWrapperDbContextOptionsBuilderExtensions.UseAwsWrapperNpgsql)] = "1";
        }

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) =>
            other is AwsWrapperDbContextOptionsExtensionInfo;

        public override AwsWrapperOptionsExtension Extension => (AwsWrapperOptionsExtension)base.Extension;

        private string? ConnectionString => this.Extension.WrapperConnectionString;
    }
}
