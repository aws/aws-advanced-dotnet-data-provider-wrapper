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

using AwsWrapperDataProvider.Driver.Configuration;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.Dialects;

public class DialectProvider
{
    private const string MySqlDataSource = "mysql";
    private const string PgDataSource = "postgres";

    private static readonly ILogger<DialectProvider> Logger = LoggerUtils.GetLogger<DialectProvider>();
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

    public IDialect GuessDialect(
        Dictionary<string, string> props,
        ConfigurationProfile? configurationProfile)
    {
        return configurationProfile?.Dialect ?? this.GuessDialect(props);
    }

    private static readonly Dictionary<(RdsUrlType UrlType, string DatasourceType), Type> DialectTypeMap = new()
    {
        { (RdsUrlType.IpAddress, PgDataSource), typeof(PgDialect) },
        { (RdsUrlType.RdsWriterCluster, PgDataSource), typeof(AuroraPgDialect) },
        { (RdsUrlType.RdsReaderCluster, PgDataSource), typeof(AuroraPgDialect) },
        { (RdsUrlType.RdsCustomCluster, PgDataSource), typeof(AuroraPgDialect) },
        { (RdsUrlType.RdsProxy, PgDataSource), typeof(RdsPgDialect) },
        { (RdsUrlType.RdsInstance, PgDataSource), typeof(RdsPgDialect) },
        { (RdsUrlType.Other, PgDataSource), typeof(PgDialect) },

        // TODO : Uncomment when Aurora Limitless DB Shard Group is supported
        // { (RdsUrlType.RdsAuroraLimitlessDbShardGroup, PgDataSource), typeof() },
        { (RdsUrlType.IpAddress, MySqlDataSource), typeof(MysqlDialect) },
        { (RdsUrlType.RdsWriterCluster, MySqlDataSource), typeof(AuroraMysqlDialect) },
        { (RdsUrlType.RdsReaderCluster, MySqlDataSource), typeof(AuroraMysqlDialect) },
        { (RdsUrlType.RdsCustomCluster, MySqlDataSource), typeof(AuroraMysqlDialect) },
        { (RdsUrlType.RdsProxy, MySqlDataSource), typeof(RdsMysqlDialect) },
        { (RdsUrlType.RdsInstance, MySqlDataSource), typeof(RdsMysqlDialect) },
        { (RdsUrlType.Other, MySqlDataSource), typeof(MysqlDialect) },

        // TODO : Uncomment when Aurora Limitless DB Shard Group is supported
        // { (RdsUrlType.RdsAuroraLimitlessDbShardGroup, MySqlDataSource), typeof() },
    };

    private readonly PluginService pluginService;
    private IDialect? dialect = null;

    public DialectProvider(PluginService pluginService)
    {
        this.pluginService = pluginService;
    }

    public IDialect GuessDialect(Dictionary<string, string> props)
    {
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
        Type targetConnectionType = Type.GetType(PropertyDefinition.TargetConnectionType.GetString(props)!) ?? throw new InvalidCastException("Target connection type not found.");
        string targetDatasourceType = ConnectionToDatasourceMap.GetValueOrDefault(targetConnectionType) ?? "unknown";
        Type dialectType = DialectTypeMap.GetValueOrDefault((rdsUrlType, targetDatasourceType), typeof(UnknownDialect));
        this.dialect = KnownDialectsByType[dialectType];
        return this.dialect;
    }

    public IDialect UpdateDialect(IDbConnection connection, IDialect currDialect)
    {
        IList<Type> dialectCandidates = currDialect.DialectUpdateCandidates;

        foreach (Type dialectCandidate in dialectCandidates)
        {
            IDialect dialect = KnownDialectsByType[dialectCandidate];
            if (dialect.IsDialect(connection))
            {
                this.dialect = dialect;
                KnownEndpointDialects.Set(this.pluginService.InitialConnectionHostSpec!.Host, dialect, EndpointCacheExpiration);
                KnownEndpointDialects.Set(connection.ConnectionString, dialect, EndpointCacheExpiration);
                return this.dialect;
            }
            else
            {
                Logger.LogDebug("Not dialect: {dialect}", dialect.GetType().FullName);
            }
        }

        if (currDialect.IsDialect(connection))
        {
            return currDialect;
        }

        throw new ArgumentException(Properties.Resources.Error_UnableToFindValidDialectType);
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
