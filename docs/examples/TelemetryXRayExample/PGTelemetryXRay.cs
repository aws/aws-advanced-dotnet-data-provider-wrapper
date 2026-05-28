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
using Amazon.XRay.Recorder.Core;
using AwsWrapperDataProvider;
using AwsWrapperDataProvider.Dialect.Npgsql;
using AwsWrapperDataProvider.Telemetry.XRay;
using Npgsql;

namespace TelemetryXRayExample;

/// <summary>
/// Sample showing how to ship trace spans from the AWS Advanced .NET Data
/// Provider Wrapper to AWS X-Ray via the in-process X-Ray SDK. The
/// accompanying <c>docker-compose.yml</c> stands up a local X-Ray daemon
/// that forwards segments to the AWS X-Ray service.
/// </summary>
public static class PGTelemetryXRay
{
    private const string ConnectionString =
        "Host=<host>;"
        + "Database=<db_name>;"
        + "Username=<user>;"
        + "Password=<password>;"
        + "EnableTelemetry=true;"
        + "TelemetryTracesBackend=XRAY;"
        + "TelemetryMetricsBackend=NONE;"
        + "TelemetrySubmitTopLevel=true;";

    public static async Task Main(string[] args)
    {
        // 1. Register the Npgsql target connection dialect.
        NpgsqlDialectLoader.Load();

        // 2. Register the X-Ray telemetry backend with DefaultTelemetryFactory.
        //    After this call, TelemetryTracesBackend=XRAY is recognized.
        XRayTelemetryLoader.Load();

        // 3. Initialize the AWS X-Ray recorder. By default this emits UDP
        //    segments to the local X-Ray daemon at 127.0.0.1:2000 and
        //    resolves AWS credentials and region through the standard SDK
        //    chain.
        AWSXRayRecorder.InitializeInstance();

        // 4. Open a connection through the wrapper and run a query.
        using AwsWrapperConnection<NpgsqlConnection> connection = new(ConnectionString);
        connection.Open();
        ExecuteQuery(connection);

        // 5. Give the X-Ray daemon a moment to flush buffered segments
        //    before the process exits.
        await Task.Delay(TimeSpan.FromSeconds(2));
    }

    private static void ExecuteQuery(AwsWrapperConnection connection)
    {
        using AwsWrapperCommand<NpgsqlCommand> command =
            connection.CreateCommand<NpgsqlCommand>();
        command.CommandText = "SELECT now() AS server_time, version() AS server_version";
        using IDataReader reader = command.ExecuteReader();
        if (reader.Read())
        {
            Console.WriteLine($"server_time:    {reader.GetDateTime(0):O}");
            Console.WriteLine($"server_version: {reader.GetString(1)}");
        }
    }
}
