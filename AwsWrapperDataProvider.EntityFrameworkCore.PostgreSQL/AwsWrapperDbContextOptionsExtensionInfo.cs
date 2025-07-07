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

namespace AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL;

public class AwsWrapperDbContextOptionsExtensionInfo : DbContextOptionsExtensionInfo
{
    public AwsWrapperDbContextOptionsExtensionInfo(AwsWrapperOptionsExtension optionsExtension) : base(optionsExtension) { }

    public override bool IsDatabaseProvider => true;

    public override string LogFragment => $"Using AWS Wrapper Provider - ConnectionString: {this.ConnectionString}";

    public override int GetServiceProviderHashCode() => (this.ConnectionString ?? string.Empty).GetHashCode();

    public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
    {
        debugInfo["AwsWrapper:ConnectionString"] = this.ConnectionString ?? string.Empty;
    }

    public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) => other is AwsWrapperDbContextOptionsExtensionInfo;

    public override AwsWrapperOptionsExtension Extension => (AwsWrapperOptionsExtension)base.Extension;

    private string? ConnectionString =>
        this.Extension.Connection == null
            ? this.Extension.ConnectionString
            : this.Extension.Connection.ConnectionString;
}
