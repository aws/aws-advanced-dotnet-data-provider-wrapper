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

using System.Data;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;

namespace AwsWrapperDataProvider.Benchmarks.Mocks;

public class MockDialect : IDialect
{
    public int DefaultPort { get; } = HostSpec.NoPort;
    public string HostAliasQuery { get; } = string.Empty;
    public string ServerVersionQuery { get; } = string.Empty;
    public IList<Type> DialectUpdateCandidates { get; } = [];

    public HostListProviderSupplier HostListProviderSupplier { get; } = (props,
        hostListProviderService,
        pluginService) => new ConnectionStringHostListProvider(props, hostListProviderService);

    public bool IsDialect(IDbConnection conn) => true;

    public void PrepareConnectionProperties(Dictionary<string, string> props, HostSpec hostSpec)
    {
        // Do nothing.
    }
}
