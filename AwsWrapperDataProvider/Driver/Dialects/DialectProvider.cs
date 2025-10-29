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

    private static readonly Dictionary<string, string> ConnectionToDatasourceMap = new()
    {
        { "Npgsql.NpgsqlConnection", PgDataSource },
        { "MySqlConnector.MySqlConnection", MySqlDataSource },
        { "MySql.Data.MySqlClient.MySqlConnection", MySqlDataSource },
    };

    private static readonly Dictionary<Type, IDialect> KnownDialectsByType = new()
    {
        { typeof(MySqlDialect), new MySqlDialect() },
        { typeof(PgDialect), new PgDialect() },
        { typeof(RdsMySqlDialect), new RdsMySqlDialect() },
        { typeof(RdsPgDialect), new RdsPgDialect() },
        { typeof(AuroraMySqlDialect), new AuroraMySqlDialect() },
        { typeof(AuroraPgDialect), new AuroraPgDialect() },
        { typeof(RdsMultiAzDbClusterMySqlDialect), new RdsMultiAzDbClusterMySqlDialect() },
        { typeof(RdsMultiAzDbClusterPgDialect), new RdsMultiAzDbClusterPgDialect() },
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
        { (RdsUrlType.IpAddress, MySqlDataSource), typeof(MySqlDialect) },
        { (RdsUrlType.RdsWriterCluster, MySqlDataSource), typeof(AuroraMySqlDialect) },
        { (RdsUrlType.RdsReaderCluster, MySqlDataSource), typeof(AuroraMySqlDialect) },
        { (RdsUrlType.RdsCustomCluster, MySqlDataSource), typeof(AuroraMySqlDialect) },
        { (RdsUrlType.RdsProxy, MySqlDataSource), typeof(RdsMySqlDialect) },
        { (RdsUrlType.RdsInstance, MySqlDataSource), typeof(RdsMySqlDialect) },
        { (RdsUrlType.Other, MySqlDataSource), typeof(MySqlDialect) },

        // TODO : Uncomment when Aurora Limitless DB Shard Group is supported
        // { (RdsUrlType.RdsAuroraLimitlessDbShardGroup, MySqlDataSource), typeof() },
    };

    private readonly PluginService pluginService;
    private IDialect? dialect = null;
    private bool canUpdate = false;

    public DialectProvider(PluginService pluginService)
    {
        this.pluginService = pluginService;
    }

    public static void ResetEndpointCache()
    {
        KnownEndpointDialects.Clear();
    }

    public IDialect GuessDialect(Dictionary<string, string> props)
    {
        this.dialect = null;
        this.canUpdate = false;

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
        string targetConnectionType = PropertyDefinition.TargetConnectionType.GetString(props)! ?? throw new InvalidCastException("Target connection type not found.");
        string targetDatasourceType = ConnectionToDatasourceMap
            .FirstOrDefault(kvp => targetConnectionType.Contains(kvp.Key))
            .Value ?? "unknown";
        Type dialectType = DialectTypeMap.GetValueOrDefault((rdsUrlType, targetDatasourceType), typeof(UnknownDialect));
        this.dialect = KnownDialectsByType[dialectType];
        Logger.LogDebug("Guessed dialect: {dialect}", this.dialect.GetType().FullName);

        this.canUpdate = true;
        return this.dialect;
    }

    public IDialect UpdateDialect(IDbConnection connection, IDialect currDialect)
    {
        Logger.LogDebug("UpdateDialect called with current dialect: {currentDialect}", currDialect.GetType().FullName);
        Logger.LogDebug("Connection type: {connectionType}", connection.GetType().FullName);
        Logger.LogDebug("Connection string: {connectionString}", connection.ConnectionString);

        if (!this.canUpdate && this.dialect != null)
        {
            Logger.LogDebug("Current dialect: {dialect}, canUpdate: {canUpdate}", this.dialect.GetType().FullName, this.canUpdate);
            return this.dialect;
        }

        IList<Type> dialectCandidates = currDialect.DialectUpdateCandidates;
        Logger.LogDebug("Testing {count} dialect candidates", dialectCandidates.Count);

        foreach (Type dialectCandidate in dialectCandidates)
        {
            Logger.LogDebug("Testing dialect candidate: {dialectCandidate}", dialectCandidate.FullName);
            IDialect dialect = KnownDialectsByType[dialectCandidate];

            try
            {
                if (dialect.IsDialect(connection))
                {
                    Logger.LogDebug("Dialect match found: {dialect}", dialect.GetType().FullName);
                    this.dialect = dialect;
                    KnownEndpointDialects.Set(this.pluginService.InitialConnectionHostSpec!.Host, dialect, EndpointCacheExpiration);
                    KnownEndpointDialects.Set(connection.ConnectionString, dialect, EndpointCacheExpiration);
                    this.canUpdate = false;
                    return this.dialect;
                }
                else
                {
                    Logger.LogDebug("Not dialect: {dialect}", dialect.GetType().FullName);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Error testing dialect candidate {dialectCandidate}: {message}", dialectCandidate.FullName, ex.Message);
            }
        }

        Logger.LogDebug("Testing current dialect: {currentDialect}", currDialect.GetType().FullName);
        try
        {
            if (currDialect.IsDialect(connection))
            {
                Logger.LogDebug("Current dialect is valid: {currentDialect}", currDialect.GetType().FullName);
                this.canUpdate = false;
                return currDialect;
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error testing current dialect {currentDialect}: {message}", currDialect.GetType().FullName, ex.Message);
        }

        Logger.LogWarning("Unable to find valid dialect type for connection. Connection type: {connectionType}, Current dialect: {currentDialect}, Candidates tested: {candidates}",
            connection.GetType().FullName,
            currDialect.GetType().FullName,
            string.Join(", ", dialectCandidates.Select(d => d.FullName)));

        if (currDialect is UnknownDialect)
        {
            throw new ArgumentException(Properties.Resources.Error_UnableToFindValidDialectType);
        }

        this.canUpdate = false;
        return currDialect;
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
