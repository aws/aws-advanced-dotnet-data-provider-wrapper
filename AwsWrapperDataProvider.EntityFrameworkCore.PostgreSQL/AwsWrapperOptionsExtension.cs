using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics;

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL
{
    public class AwsWrapperOptionsExtension : RelationalOptionsExtension
    {
        private AwsWrapperDbContextOptionsExtensionInfo? _info;
        private IDbContextOptionsExtension _wrappedExtension;

        // Add more AWS Wrapper settings here if needed

        public AwsWrapperOptionsExtension(IDbContextOptionsExtension wrappedExtension) 
        { 
            this._wrappedExtension = wrappedExtension;
        }

        protected internal AwsWrapperOptionsExtension(AwsWrapperOptionsExtension copyFrom) : base(copyFrom) 
        {
            this._wrappedExtension = copyFrom._wrappedExtension;
        }

        public override void ApplyServices(IServiceCollection services)
        {
            var wrappedServiceCollection = new WrappedServiceCollection();
            this._wrappedExtension.ApplyServices(wrappedServiceCollection);

            ServiceDescriptor? targetRelationalConnectionServiceDescriptor = wrappedServiceCollection
                .Where(x => x.ServiceType == typeof(IRelationalConnection))
                .FirstOrDefault();
            Debug.Assert(targetRelationalConnectionServiceDescriptor != null);

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
                .Where(x => x.ServiceType != typeof(IDatabaseProvider) && x.ServiceType != typeof(IRelationalConnection)))
            {
                services.Add(serviceDescriptor);
            }
        }

        protected override RelationalOptionsExtension Clone() => new AwsWrapperOptionsExtension(this._wrappedExtension);

        /// <inheritdoc />
        public override DbContextOptionsExtensionInfo Info => _info ??= new AwsWrapperDbContextOptionsExtensionInfo(this);

        public IDbContextOptionsExtension WrappedExtension 
        {
            get => this._wrappedExtension;
            set => this._wrappedExtension = value;
        }
    }

}
