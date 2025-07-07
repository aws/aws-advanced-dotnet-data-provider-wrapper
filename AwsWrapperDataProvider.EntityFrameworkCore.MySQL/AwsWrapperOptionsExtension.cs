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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AwsWrapperDataProvider.EntityFrameworkCore.MySQL;

public class AwsWrapperOptionsExtension : IDbContextOptionsExtension
{
    private AwsWrapperDbContextOptionsExtensionInfo? info;

    public AwsWrapperOptionsExtension()
    {
    }

    public void ApplyServices(IServiceCollection services)
    {
        var builder = new EntityFrameworkRelationalServicesBuilder(services);

        var targetRelationalConnectionServiceDescriptor = services.FirstOrDefault(x => x.ServiceType == typeof(IRelationalConnection));

        Type? targetRelationalConnectionType = targetRelationalConnectionServiceDescriptor?.ImplementationType;
        Func<IServiceProvider, object>? targetRelationalConnectionImplementationFactory = targetRelationalConnectionServiceDescriptor?.ImplementationFactory;
        if (targetRelationalConnectionType != null)
        {
            builder.TryAddProviderSpecificServices(m => m.TryAddScoped<IAwsWrapperRelationalConnection>(p => new AwsWrapperRelationalConnection(
                p.GetRequiredService<RelationalConnectionDependencies>(),
                (IRelationalConnection)p.GetRequiredService(targetRelationalConnectionType))));
        }
        else if (targetRelationalConnectionImplementationFactory != null)
        {
            builder.TryAddProviderSpecificServices(m => m.TryAddScoped<IAwsWrapperRelationalConnection>(p => new AwsWrapperRelationalConnection(
                p.GetRequiredService<RelationalConnectionDependencies>(),
                (IRelationalConnection)targetRelationalConnectionImplementationFactory.Invoke(p))));
        }
        else
        {
            throw new Exception("not implemented");
        }

        services.Replace(new ServiceDescriptor(typeof(IRelationalConnection), p => p.GetRequiredService<IAwsWrapperRelationalConnection>(), ServiceLifetime.Scoped));
    }

    public void Validate(IDbContextOptions options)
    {
    }

    /// <inheritdoc />
    public DbContextOptionsExtensionInfo Info => this.info ??= new AwsWrapperDbContextOptionsExtensionInfo(this);

    private sealed class AwsWrapperDbContextOptionsExtensionInfo : DbContextOptionsExtensionInfo
    {
        public AwsWrapperDbContextOptionsExtensionInfo(AwsWrapperOptionsExtension optionsExtension) : base(optionsExtension) { }

        public override bool IsDatabaseProvider => false;

        public override string LogFragment => $"Using AWS Wrapper Provider ";

        public override int GetServiceProviderHashCode() => 0;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            debugInfo["AwsWrapper:" + nameof(AwsWrapperDbContextOptionsBuilderExtensions.UseAwsWrapper)] = "1";
        }

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) =>
            other is AwsWrapperDbContextOptionsExtensionInfo;

        public override AwsWrapperOptionsExtension Extension => (AwsWrapperOptionsExtension)base.Extension;
    }
}
