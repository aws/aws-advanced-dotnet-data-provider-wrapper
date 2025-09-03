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

namespace AwsWrapperDataProvider.Driver.Utils;

public static class PropertyDefinition
{
    public static readonly AwsWrapperProperty Server =
        new("Server", null, "MySql connection url.");

    public static readonly AwsWrapperProperty Host =
        new("Host", null, "Postgres connection url.");

    public static readonly AwsWrapperProperty Port =
        new("Port", null, "Connection port.");

    public static readonly AwsWrapperProperty User =
        new("Username", null, "The user name that the driver will use to connect to database.");

    public static readonly AwsWrapperProperty UserId =
        new("User ID", null, "The user name and host name that the driver will use to connect to database.");

    public static readonly AwsWrapperProperty Password =
        new("Password", null, "The password that the driver will use to connect to database.");

    public static readonly AwsWrapperProperty TargetConnectionType =
        new("TargetConnectionType", null, "Driver target connection type.");

    public static readonly AwsWrapperProperty TargetCommandType =
        new("TargetCommandType", null, "Driver target command type.");

    public static readonly AwsWrapperProperty TargetDialect =
        new("CustomDialect", null, "Custom dialect type. Should be AssemblyQualifiedName of class implementing IDialect.");

    public static readonly AwsWrapperProperty CustomTargetConnectionDialect =
        new("CustomTargetConnectionDialect", null, "Custom target connection dialect type. Should be AssemblyQualifiedName of class implementing ITargetConnectionDialect.");

    public static readonly AwsWrapperProperty Plugins = new(
        "Plugins",
        "efm,failover",
        "Comma separated list of connection plugin codes");

    public static readonly AwsWrapperProperty AutoSortPluginOrder = new(
        "AutoSortPluginOrder",
        "true",
        "This flag is enabled by default, meaning that the plugins order will be automatically adjusted. Disable it at your own risk or if you really need plugins to be executed in a particular order.");

    public static readonly AwsWrapperProperty SingleWriterConnectionString = new(
        "SingleWriterConnectionString",
        "false",
        "Set to true if you are providing a connection string with multiple comma-delimited hosts and your cluster has only one writer. The writer must be the first host in the connection string.");

    public static readonly AwsWrapperProperty IamHost =
        new("IamHost", null, "Overrides the host that is used to generate the IAM token.");

    public static readonly AwsWrapperProperty IamDefaultPort =
        new("IamDefaultPort", "-1", "Overrides default port that is used to generate the IAM token.");

    public static readonly AwsWrapperProperty IamRegion =
        new("IamRegion", null, "Overrides AWS region that is used to generate the IAM token.");

    public static readonly AwsWrapperProperty IamExpiration =
        new("IamExpiration", "870", "IAM token cache expiration in seconds.");

    public static readonly AwsWrapperProperty IamRoleArn = new(
        "IamRoleArn", null, "The ARN of the IAM Role that is to be assumed.");

    public static readonly AwsWrapperProperty IamIdpArn = new(
        "IamIdpArn", null, "The ARN of the Identity Provider");

    public static readonly AwsWrapperProperty ClusterTopologyRefreshRateMs = new(
        "ClusterTopologyRefreshRateMs",
        "30000",
        "Cluster topology refresh rate in milliseconds. The cached topology for the cluster will be invalidated after the specified time, after which it will be updated during the next interaction with the connection.");

    public static readonly AwsWrapperProperty ClusterInstanceHostPattern = new(
        "ClusterInstanceHostPattern",
        null,
        "The cluster instance DNS pattern that will be used to build a complete instance endpoint. A \"?\" character in this pattern should be used as a placeholder for cluster instance names. This pattern is required to be specified for IP address or custom domain connections to AWS RDS clusters. Otherwise, if unspecified, the pattern will be automatically created for AWS RDS clusters.");

    public static readonly AwsWrapperProperty ClusterId = new(
        "ClusterId",
        string.Empty,
        "A unique identifier for the cluster. Connections with the same cluster id share a cluster topology cache. If unspecified, a cluster id is automatically created for AWS RDS clusters.");

    public static readonly AwsWrapperProperty SecretsManagerSecretId = new(
        "SecretsManagerSecretId", null, "The name or the ARN of the secret to retrieve.");

    public static readonly AwsWrapperProperty SecretsManagerRegion = new(
        "SecretsManagerRegion", "us-east-1", "The region of the secret to retrieve.");

    public static readonly AwsWrapperProperty SecretsManagerExpirationSecs = new(
        "SecretsManagerExpirationSec", "870", "The time in seconds that secrets are cached for.");

    public static readonly AwsWrapperProperty SecretsManagerEndpoint = new(
        "SecretsManagerEndpoint", null, "The endpoint of the secret to retrieve.");

    public static readonly AwsWrapperProperty OpenConnectionRetryTimeoutMs = new(
        "OpenConnectionRetryTimeoutMs", "30000", "Maximum allowed time for the retries opening a connection.");

    public static readonly AwsWrapperProperty OpenConnectionRetryIntervalMs = new(
        "OpenConnectionRetryIntervalMs", "1000", "Time between each retry of opening a connection.");

    public static readonly AwsWrapperProperty VerifyOpenedConnectionType = new(
        "VerifyOpenedConnectionType", null, "Force to verify an opened connection to be either a writer or a reader.");

    public static readonly AwsWrapperProperty IdpEndpoint = new(
        "IdpEndpoint", null, "The hosting URL of the Identity Provider");

    public static readonly AwsWrapperProperty IdpPort = new(
        "IdpPort", "443", "The hosting port of Identity Provider");

    public static readonly AwsWrapperProperty IdpUsername = new(
        "IdpUsername", null, "The federated user name");

    public static readonly AwsWrapperProperty IdpPassword = new(
        "IdpPassword", null, "The federated user password");

    public static readonly AwsWrapperProperty RelayingPartyId = new(
        "RpIdentifier", "urn:amazon:webservices", "The relaying party identifier");

    public static readonly AwsWrapperProperty DbUser = new(
        "DbUser", null, "The database user used to access the database");

    public static readonly AwsWrapperProperty HttpClientConnectTimeout = new AwsWrapperProperty(
        "HttpClientConnectTimeout", "10000", "The connect timeout value in milliseconds for the HttpClient used by the federated auth and OKTA plugins.");

    // Failover Plugin Properties
    public static readonly AwsWrapperProperty FailoverTimeoutMs = new(
        "FailoverTimeoutMs", "300000", "Maximum allowed time for the failover process in milliseconds.");

    public static readonly AwsWrapperProperty FailoverMode = new(
        "FailoverMode", null, "Set node role to follow during failover. Valid values: StrictWriter, StrictReader, ReaderOrWriter.");

    public static readonly AwsWrapperProperty ReaderHostSelectorStrategy = new(
        "ReaderHostSelectorStrategy", "Random", "The strategy that should be used to select a new reader host while opening a new connection.");

    public static readonly AwsWrapperProperty EnableConnectFailover = new(
        "EnableConnectFailover", "false", "Enable/disable cluster-aware failover if the initial connection to the database fails due to a network exception.");

    public static readonly AwsWrapperProperty SkipFailoverOnInterruptedThread = new(
        "SkipFailoverOnInterruptedThread", "false", "Enable to skip failover if the current thread is interrupted.");
    public static readonly AwsWrapperProperty ClusterTopologyHighRefreshRateMs = new(
        "ClusterTopologyHighRefreshRateMs", "100", "Cluster topology high refresh rate in milliseconds.");

    // Connection Timeout Properties
    public static readonly AwsWrapperProperty SocketTimeout = new(
        "SocketTimeout", "5000", "The socket timeout value in milliseconds for database connections.");

    public static readonly AwsWrapperProperty ConnectTimeout = new(
        "ConnectTimeout", "5000", "The connection timeout value in milliseconds for establishing database connections.");

    // Host Selector Stratagy Properties
    public static readonly AwsWrapperProperty RoundRobinHostWeightPairs = new(
        "RoundRobinHostWeightPairs",
        null,
        "Comma separated list of database host-weight pairs in the format of `<host>:<weight>`.");

    public static readonly AwsWrapperProperty RoundRobinDefaultWeight = new(
        "RoundRobinDefaultWeight",
        "1",
        "The default weight for any hosts that have not been configured with the `roundRobinHostWeightPairs` parameter.");

    public static readonly AwsWrapperProperty MonitorDisposalTimeMs = new(
        "MonitorDisposalTime",
        "600000", // 10min
        "Interval in milliseconds for a monitor to be considered inactive and to be disposed.");

    public static readonly AwsWrapperProperty FailureDetectionEnabled = new(
        "FailureDetectionEnabled",
        "true",
        "Enable failure detection logic (aka node monitoring thread).");

    public static readonly AwsWrapperProperty FailureDetectionTime = new(
        "FailureDetectionTime",
        "30000",
        "Interval in milliseconds between sending SQL to the server and the first probe to database node.");

    public static readonly AwsWrapperProperty FailureDetectionInterval = new(
        "FailureDetectionInterval",
        "5000",
        "Interval in milliseconds between probes to database node.");

    public static readonly AwsWrapperProperty FailureDetectionCount = new(
        "FailureDetectionCount",
        "3",
        "Number of failed connection checks before considering database node unhealthy.");

    public static readonly AwsWrapperProperty AppId = new(
        "AppId",
        null,
        "The ID of the AWS application configured on Okta");

    /// <summary>
    /// A set of AwsWrapperProperties that is used by the wrapper and should not be passed to the target driver.
    /// </summary>
    public static readonly HashSet<AwsWrapperProperty> InternalWrapperProperties = [
        TargetConnectionType,
        TargetCommandType,
        CustomTargetConnectionDialect,
        TargetDialect,
        Plugins,
        AutoSortPluginOrder,
        SingleWriterConnectionString,
        IamHost,
        IamDefaultPort,
        IamRegion,
        IamExpiration,
        IamRoleArn,
        IamIdpArn,
        SecretsManagerSecretId,
        SecretsManagerRegion,
        SecretsManagerExpirationSecs,
        SecretsManagerEndpoint,
        IdpEndpoint,
        IdpPort,
        IdpUsername,
        IdpPassword,
        RelayingPartyId,
        DbUser,
        HttpClientConnectTimeout,
        ClusterTopologyRefreshRateMs,
        ClusterInstanceHostPattern,
        ClusterId,
        OpenConnectionRetryTimeoutMs,
        OpenConnectionRetryIntervalMs,
        VerifyOpenedConnectionType,
        AppId,

        // Failover Plugin Properties
        FailoverTimeoutMs,
        FailoverMode,
        ReaderHostSelectorStrategy,
        EnableConnectFailover,
        SkipFailoverOnInterruptedThread,
        ClusterTopologyHighRefreshRateMs,

        // Host Selector Stratagy Properties
        RoundRobinHostWeightPairs,
        RoundRobinDefaultWeight,

        // EFM2 Plugin Properties
        MonitorDisposalTimeMs,
        FailureDetectionEnabled,
        FailureDetectionTime,
        FailureDetectionInterval,
        FailureDetectionCount,
    ];

    public static string GetConnectionUrl(Dictionary<string, string> props)
    {
        return Server.GetString(props) ?? Host.GetString(props) ?? throw new ArgumentException("Connection url is missing.");
    }
}
