using Microsoft.EntityFrameworkCore.Infrastructure;

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL
{
    public class AwsWrapperDbContextOptionsExtensionInfo : DbContextOptionsExtensionInfo
    {
        public AwsWrapperDbContextOptionsExtensionInfo(AwsWrapperOptionsExtension optionsExtension) : base(optionsExtension) { }

        public override bool IsDatabaseProvider => false;

        public override string LogFragment => 
            $"Using AWS Wrapper Provider - ConnectionString: {ConnectionString}";

        public override int GetServiceProviderHashCode() => (ConnectionString ?? "").GetHashCode();

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            debugInfo["AwsWrapper:ConnectionString"] = ConnectionString ?? "";
        }

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) => 
            other is AwsWrapperDbContextOptionsExtensionInfo;

        public override AwsWrapperOptionsExtension Extension => (AwsWrapperOptionsExtension)base.Extension;

        private string? ConnectionString => 
            Extension.Connection == null 
                ? Extension.ConnectionString 
                : Extension.Connection.ConnectionString;
    }
}
