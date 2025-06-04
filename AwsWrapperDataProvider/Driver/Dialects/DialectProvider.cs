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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class DialectProvider
{
    private const string MySqlDataSource = "mysql";
    private const string PgDataSource = "postgres";

    private static readonly MemoryCache KnownEndpointDialects = new(new MemoryCacheOptions());
    private static readonly TimeSpan EndpointCacheExpiration = TimeSpan.FromHours(24);

    private static readonly Dictionary<Type, string> ConnectionToDatasourceMap = new()
    {
        { typeof(Npgsql.NpgsqlConnection), PgDataSource },
        { typeof(MySqlConnector.MySqlConnection), MySqlDataSource },
        { typeof(MySql.Data.MySqlClient.MySqlConnection), MySqlDataSource },
    };

    private static readonly Dictionary<Type, IDialect> KnownDialectsByType = new()
    {
        { typeof(MysqlDialect), new MysqlDialect() },
        { typeof(PgDialect), new PgDialect() },
        { typeof(RdsMysqlDialect), new RdsMysqlDialect() },
        { typeof(RdsPgDialect), new RdsPgDialect() },
        { typeof(AuroraMysqlDialect), new AuroraMysqlDialect() },
        { typeof(AuroraPgDialect), new AuroraPgDialect() },
        { typeof(UnknownDialect), new UnknownDialect() },
    };

    private readonly PluginService pluginService;
    private bool canUpdate = false;
    private IDialect? dialect = null;

    public DialectProvider(PluginService pluginService)
    {
        this.pluginService = pluginService;
    }

    public IDialect GuessDialect(Dictionary<string, string> props)
    {
        this.canUpdate = false;
        this.dialect = null;

        // Check for custom dialect in properties
        if (PropertyDefinition.TargetDialect.GetString(props) is { } customDialectTypeName &&
            !string.IsNullOrEmpty(customDialectTypeName))
        {
            // Try to find and instantiate the custom dialect type
            Type? customDialectType = Type.GetType(customDialectTypeName);

            return GetDialectFromType(customDialectType) ??
                   throw new InvalidOperationException($"Failed to instantiate custom dialect type '{customDialectTypeName}'");
        }

        string host = PropertyDefinition.GetConnectionUrl(props);
        IList<HostSpec> hosts = ConnectionPropertiesUtils.GetHostsFromProperties(
            props,
            this.pluginService.HostSpecBuilder,
            true);
        if (hosts.Count != 0)
        {
            host = hosts.First().Host;
        }

        if (KnownEndpointDialects.TryGetValue(host, out IDialect? cachedDialect) && cachedDialect != null)
        {
            this.dialect = cachedDialect;
            return this.dialect!;
        }

        RdsUrlType rdsUrlType = RdsUtils.IdentifyRdsType(host);
        Type targetConnectionType = Type.GetType(PropertyDefinition.TargetConnectionType.GetString(props)!) ??
                                    throw new InvalidCastException("Target connection type not found.");
        string targetDatasourceType = ConnectionToDatasourceMap.GetValueOrDefault(targetConnectionType) ?? "unknown";
        if (targetDatasourceType == MySqlDataSource)
        {
            if (rdsUrlType.IsRdsCluster)
            {
                // TODO change to true once supports RDS_MULTI_AZ_MYSQL_CLUSTER
                this.canUpdate = false;
                this.dialect = KnownDialectsByType[typeof(AuroraMysqlDialect)];
                return this.dialect;
            }

            if (rdsUrlType.IsRds)
            {
                this.canUpdate = true;
                this.dialect = KnownDialectsByType[typeof(RdsMysqlDialect)];
                return this.dialect;
            }

            this.canUpdate = true;
            this.dialect = KnownDialectsByType[typeof(MysqlDialect)];
            return this.dialect;
        }

        if (targetDatasourceType == PgDataSource)
        {
            if (rdsUrlType.IsRdsCluster)
            {
                // TODO change to true once supports RDS_MULTI_AZ_PG_CLUSTER
                this.canUpdate = false;
                this.dialect = KnownDialectsByType[typeof(AuroraPgDialect)];
                return this.dialect;
            }

            if (rdsUrlType.IsRds)
            {
                this.canUpdate = true;
                this.dialect = KnownDialectsByType[typeof(RdsPgDialect)];
                return this.dialect;
            }

            this.canUpdate = true;
            this.dialect = KnownDialectsByType[typeof(PgDialect)];
            return this.dialect;
        }

        this.canUpdate = true;
        this.dialect = KnownDialectsByType[typeof(UnknownDialect)];
        return this.dialect;
    }

    public IDialect UpdateDialect(IDbConnection connection, IDialect currDialect)
    {
        if (!this.canUpdate)
        {
            return this.dialect!;
        }

        IList<Type> dialectCandidates = currDialect.DialectUpdateCandidates;

        foreach (Type dialectCandidate in dialectCandidates)
        {
            IDialect dialect = KnownDialectsByType[dialectCandidate];
            if (dialect.IsDialect(connection))
            {
                this.canUpdate = false;
                this.dialect = dialect;
                KnownEndpointDialects.Set(this.pluginService.InitialConnectionHostSpec!.Host, dialect, EndpointCacheExpiration);
                KnownEndpointDialects.Set(connection.ConnectionString, dialect, EndpointCacheExpiration);
                return this.dialect;
            }
        }

        if (currDialect.IsDialect(connection))
        {
            return currDialect;
        }

        throw new ArgumentException("Unable to find valid dialect type for connection.");
    }

    private static IDialect? GetDialectFromType(Type? dialectType)
    {
        if (dialectType != null && typeof(IDialect).IsAssignableFrom(dialectType))
        {
            KnownDialectsByType.TryGetValue(dialectType, out IDialect? dialect);

            if (dialect == null)
            {
                dialect = (IDialect)Activator.CreateInstance(dialectType)!;
                KnownDialectsByType[dialectType] = dialect;
            }

            return dialect;
        }

        return null;
    }
}
