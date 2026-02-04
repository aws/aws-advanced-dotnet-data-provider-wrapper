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
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Plugins.Limitless;

public class LimitlessQueryHelper
{
    private const int DefaultQueryTimeoutMs = 5000;

    private static readonly ILogger<LimitlessQueryHelper> Logger = LoggerUtils.GetLogger<LimitlessQueryHelper>();

    private readonly IPluginService _pluginService;

    public LimitlessQueryHelper(IPluginService pluginService)
    {
        this._pluginService = pluginService;
    }

    public virtual async Task<IList<HostSpec>> QueryForLimitlessRouters(DbConnection conn, int hostPortToMap)
    {
        var dialect = this._pluginService.Dialect;
        if (dialect is not IAuroraLimitlessDialect limitlessDialect)
        {
            throw new NotSupportedException(string.Format(Resources.Error_UnsupportedDialectOrDatabase, dialect?.GetType().Name));
        }

        try
        {
            using DbCommand command = conn.CreateCommand();
            command.CommandText = limitlessDialect.LimitlessRouterEndpointQuery;
            command.CommandTimeout =
                command.CommandTimeout == 0
                    ? DefaultQueryTimeoutMs / 1000 // Convert to seconds
                    : command.CommandTimeout;

            using var reader = await command.ExecuteReaderAsync();
            return this.MapResultSetToHostSpecList(reader, hostPortToMap);
        }
        catch (DbException e) when (this._pluginService.TargetConnectionDialect.IsSyntaxError(e))
        {
            throw new InvalidOperationException(Resources.Error_InvalidQuery, e);
        }
    }

    private IList<HostSpec> MapResultSetToHostSpecList(DbDataReader reader, int hostPortToMap)
    {
        List<HostSpec> hosts = new();
        while (reader.Read())
        {
            HostSpec host = this.CreateHost(reader, hostPortToMap);
            hosts.Add(host);
        }

        return hosts;
    }

    private HostSpec CreateHost(DbDataReader reader, int hostPortToMap)
    {
        string hostName = reader.GetString(0);
        float cpu = reader.GetFloat(1);

        long weight = (long)(10 - Math.Floor(10 * cpu));

        if (weight < 1 || weight > 10)
        {
            weight = 1; // default to 1
            Logger.LogWarning(Resources.LimitlessQueryHelper_CreateHost_InvalidRouterLoad, hostName, cpu);
        }

        return this._pluginService.HostSpecBuilder
            .WithHost(hostName)
            .WithPort(hostPortToMap)
            .WithRole(HostRole.Writer)
            .WithAvailability(HostAvailability.Available)
            .WithWeight(weight)
            .WithHostId(hostName)
            .Build();
    }
}
