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

namespace AwsWrapperDataProvider.Driver.Dialects;

public class RdsMySqlDialect : MySqlDialect
{
    public override IList<Type> DialectUpdateCandidates { get; } =
    [
        typeof(AuroraMySqlDialect),
        typeof(RdsMultiAzDbClusterMySqlDialect),
    ];

    public override bool IsDialect(IDbConnection conn)
    {
        if (base.IsDialect(conn))
        {
            return false;
        }

        try
        {
            using IDbCommand command = conn.CreateCommand();
            command.CommandText = this.ServerVersionQuery;
            using IDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                int columnCount = reader.FieldCount;
                for (int i = 0; i < columnCount; i++)
                {
                    string columnValue = reader.GetString(i);
                    if (string.Equals(columnValue, "Source distribution", StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
            }
        }
        catch (DbException)
        {
            // ignored
        }

        return false;
    }
}
