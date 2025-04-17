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


using System.Data.Common;
using AwsWrapperDataProvider.driver.exceptions;
using AwsWrapperDataProvider.driver.hostListProviders;

namespace AwsWrapperDataProvider.driver.dialects;

public interface IDialect
{
    int DefaultPort { get; }

    // IExceptionHandler ExceptionHandler { get; }

    string HostAliasQuery { get; }

    string ServerVersionQuery { get; }

    IList<string> DialectUpdateCandidates { get; }

    HostListProviderSupplier HostListProviderSupplier { get; }

    // FailOverRestrictions FailOverRestrictions { get; }

    bool IsDialect(DbConnection conn);
}

public delegate IHostListProvider HostListProviderSupplier(
    Dictionary<string, string> props,
    string? initialUrl,
    IHostListProviderService hostListProviderService,
    IPluginService pluginService
);