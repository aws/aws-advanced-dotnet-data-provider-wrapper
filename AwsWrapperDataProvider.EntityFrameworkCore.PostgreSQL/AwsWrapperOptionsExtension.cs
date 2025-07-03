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
using Microsoft.Extensions.DependencyInjection;

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL;

public class AwsWrapperOptionsExtension : RelationalOptionsExtension, IDbContextOptionsExtension
{
    private AwsWrapperDbContextOptionsExtensionInfo? _info;

    // Add more AWS Wrapper settings here if needed

    public AwsWrapperOptionsExtension(IDbContextOptionsExtension wrappedExtension)
    {
        this.WrappedExtension = wrappedExtension;
    }

    protected internal AwsWrapperOptionsExtension(AwsWrapperOptionsExtension copyFrom) : base(copyFrom)
    {
        this.WrappedExtension = copyFrom.WrappedExtension;
    }

    public override void ApplyServices(IServiceCollection services)
    {
        var wrappedServiceCollection = new WrappedServiceCollection();
        this.WrappedExtension.ApplyServices(wrappedServiceCollection);

        ServiceDescriptor? targetRelationalConnectionServiceDescriptor = wrappedServiceCollection
            .FirstOrDefault(x => x.ServiceType == typeof(IRelationalConnection));

        Type? targetRelationalConnectionType = targetRelationalConnectionServiceDescriptor?.ImplementationType;
        Func<IServiceProvider, object>? targetRelationalConnectionImplementationFactory = targetRelationalConnectionServiceDescriptor?.ImplementationFactory;

        // add IDatabaseProvider
        services.Add(new ServiceDescriptor(typeof(IDatabaseProvider), typeof(DatabaseProvider<AwsWrapperOptionsExtension>), ServiceLifetime.Singleton));

        // add IRelationalConnection
        services.Add(new ServiceDescriptor(typeof(IRelationalConnection), p => p.GetRequiredService<IAwsWrapperRelationalConnection>(), ServiceLifetime.Scoped));
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
            throw new Exception("not implemented");
        }

        // add all other service descriptors
        foreach (var serviceDescriptor in wrappedServiceCollection
            .Where(x => x.ServiceType != typeof(IDatabaseProvider)
                && x.ServiceType != typeof(IRelationalConnection)))
        {
            services.Add(serviceDescriptor);
        }
    }

    protected override RelationalOptionsExtension Clone() => new AwsWrapperOptionsExtension(this);

    /// <inheritdoc />
    public override DbContextOptionsExtensionInfo Info => this._info ??= new AwsWrapperDbContextOptionsExtensionInfo(this);

    public IDbContextOptionsExtension WrappedExtension { get; set; }
}
