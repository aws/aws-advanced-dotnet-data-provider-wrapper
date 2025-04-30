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

namespace AwsWrapperDataProvider.Driver.Dialects;

public static class DialectProvider
{
    private static readonly Dictionary<DialectCodes, Type> ConnectionToDialectMap = new()
    {
        { DialectCodes.Pg, typeof(PgDialect) },
        { DialectCodes.Mysql, typeof(MysqlDialect) },
    };

    public static IDialect GetDialect(Type connectionType, Dictionary<string, string> props)
    {
        // TODO: is stub implementation, implement function
        return new PgDialect();
    }
}
