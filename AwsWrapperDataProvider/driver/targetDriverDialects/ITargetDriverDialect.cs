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
using AwsWrapperDataProvider.driver.hostInfo;

namespace AwsWrapperDataProvider.driver.targetDriverDialects;

// TODO: find out if we even need this :P
public interface ITargetDriverDialect
{
    bool IsDialect(Type connectionType);

    string PrepareConnectionString(HostSpec hostSpec, Dictionary<string, string> props);

    // void PrepareDataSource(DbConnection connection, HostSpec hostSpec, Dictionary<string, string> props);

    // bool Ping(DbConnection connection);

    // ISet<string> GetAllowedOnConnectionMethodNames();

    // string GetSqlState(Exception exception);
}