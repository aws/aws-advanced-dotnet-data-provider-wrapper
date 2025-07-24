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

namespace AwsWrapperDataProvider.Tests.Container.Utils;
public class ConnectionStringHelper
{
    public static string GetUrl(string host, int port, string username, string password, string dbName)
    {
        return $"Server={host};Port={port};User Id={username};Password={password};Database={dbName};Pooling=true;Min Pool Size=0;Max Pool Size=100;Connection Idle Lifetime=300;Ssl Mode=Preferred;";
    }
}
