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
using System.Data.Common;
using AwsWrapperDataProvider.Driver.HostInfo;

namespace AwsWrapperDataProvider.Driver.HostListProviders;

public interface IHostListProvider
{
    IList<HostSpec> Refresh();

    IList<HostSpec> Refresh(IDbConnection connection);

    IList<HostSpec> ForceRefresh();

    IList<HostSpec> ForceRefresh(IDbConnection connection);

    HostRole GetHostRole(IDbConnection connection);

    string GetClusterId();

    HostSpec? IdentifyConnection(DbConnection connection, DbTransaction? transaction = null);
}
