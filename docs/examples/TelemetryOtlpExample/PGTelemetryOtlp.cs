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
using AwsWrapperDataProvider;
using AwsWrapperDataProvider.Dialect.Npgsql;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using Npgsql;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace TelemetryOtlpExample;

/// <summary>
/// Sample showing how to ship trace spans from the AWS Advanced .NET Data
/// Provider Wrapper through the OpenTelemetry SDK to an OTLP-compatible
/// backend. The accompanying <c>docker-compose.yml</c> stands up an AWS
/// Distro for OpenTelemetry (ADOT) Collector that translates the spans
/// into AWS X-Ray segments.
/// </summary>
public static class PGTelemetryOtlp
{
    private const string ConnectionString =
        "Host=<host>;"
        + "Database=<db_name>;"
        + "Username=<user>;"
        + "Password=<password>;"
        + "EnableTelemetry=true;"
        + "TelemetryTracesBackend=OTLP;"
        + "TelemetryMetricsBackend=NONE;"
        + "TelemetrySubmitTopLevel=true;";

    public static async Task Main(string[] args)
    {
        // 1. Register the Npgsql target connection dialect.
        NpgsqlDialectLoader.Load();

        // 2. Configure the OpenTelemetry SDK before any wrapper code runs.
        //    The TracerProvider is process-wide and is disposed at scope
        //    exit, which flushes pending spans through the OTLP exporter.
        //
        //    AddSource(...) wires the SDK to the ActivitySource the wrapper
        //    emits to. The name comes from
        //    OtlpTelemetryFactory.InstrumentationName.
        //
        //    AddXRayTraceId() makes the SDK produce trace IDs that the
        //    ADOT Collector's awsxrayexporter accepts. Drop this call if
        //    your eventual backend is not AWS X-Ray.
        using TracerProvider tracerProvider = Sdk.CreateTracerProviderBuilder()
            .AddXRayTraceId()
            .SetResourceBuilder(ResourceBuilder.CreateDefault()
                .AddService("aws-wrapper-otlp-example"))
            .AddSource(OtlpTelemetryFactory.InstrumentationName)
            .AddOtlpExporter(opts =>
            {
                opts.Endpoint = new Uri("http://localhost:4318/v1/traces");
                opts.Protocol = OtlpExportProtocol.HttpProtobuf;
            })
            .Build()!;

        // 3. Open a connection through the wrapper and run a query.
        using AwsWrapperConnection<NpgsqlConnection> connection = new(ConnectionString);
        connection.Open();
        ExecuteQuery(connection);

        // 4. Force flush before the TracerProvider is disposed at scope
        //    exit so the OTLP exporter ships any buffered spans before the
        //    process ends.
        tracerProvider.ForceFlush(5_000);

        await Task.CompletedTask;
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
