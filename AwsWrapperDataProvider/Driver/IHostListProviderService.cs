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
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;

namespace AwsWrapperDataProvider.Driver;

/// <summary>
/// Interface for the host list provider service that manages host list providers.
/// </summary>
public interface IHostListProviderService
{
    IDialect Dialect { get; }

    IHostListProvider HostListProvider { get; set; }

    DbConnection? CurrentConnection { get; }

    HostSpec CurrentHostSpec { get; }

    HostSpec InitialConnectionHostSpec { get; set; }

    HostSpecBuilder HostSpecBuilder { get; }

    /// <summary>
    /// Checks if the current host list provider is static.
    /// </summary>
    /// <returns>True if the host list provider is static, false otherwise.</returns>
    bool IsStaticHostListProvider();
}
