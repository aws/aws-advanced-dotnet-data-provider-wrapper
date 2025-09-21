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
using AwsWrapperDataProvider.Driver.Utils;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

public static class DialectDebugHelper
{
    public static void DebugDialectDetection(string connectionString, IDbConnection connection)
    {
        Console.WriteLine("[DIALECT DEBUG] Starting dialect detection debug");
        Console.WriteLine($"[DIALECT DEBUG] Connection string: {connectionString}");
        Console.WriteLine($"[DIALECT DEBUG] Connection type: {connection.GetType().FullName}");
        Console.WriteLine($"[DIALECT DEBUG] Connection state: {connection.State}");

        try
        {
            var props = ConnectionPropertiesUtils.ParseConnectionStringParameters(connectionString);
            Console.WriteLine("[DIALECT DEBUG] Parsed properties:");
            foreach (var prop in props)
            {
                Console.WriteLine($"  {prop.Key}: {prop.Value}");
            }

            // Test individual dialects
            var dialects = new IDialect[]
            {
                new PgDialect(),
                new RdsPgDialect(),
                new AuroraPgDialect()
            };

            foreach (var dialect in dialects)
            {
                try
                {
                    bool isMatch = dialect.IsDialect(connection);
                    Console.WriteLine($"[DIALECT DEBUG] {dialect.GetType().Name}: {(isMatch ? "MATCH" : "NO MATCH")}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[DIALECT DEBUG] {dialect.GetType().Name}: ERROR - {ex.Message}");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[DIALECT DEBUG] Error during debug: {ex.Message}");
        }
    }
}
