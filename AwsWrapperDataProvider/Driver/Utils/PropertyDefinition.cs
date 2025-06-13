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

    public static readonly AwsWrapperProperty IamHost = new(
        "iamHost", null, "Overrides the host that is used to generate the IAM token.");

    public static readonly AwsWrapperProperty IamDefaultPort = new(
        "iamDefaultPort", "-1", "Overrides default port that is used to generate the IAM token.");

    public static readonly AwsWrapperProperty IamRegion = new(
        "iamRegion", null, "Overrides AWS region that is used to generate the IAM token.");

    public static readonly AwsWrapperProperty IamExpiration = new(
        "iamExpiration", "870", "IAM token cache expiration in seconds.");

    public static readonly AwsWrapperProperty IamRoleArn = new(
        "iamRoleArn", null, "The ARN of the IAM Role that is to be assumed.");

    public static readonly AwsWrapperProperty IamIdpArn = new(
        "iamIdpArn", null, "The ARN of the Identity Provider");

    public static readonly AwsWrapperProperty ClusterTopologyRefreshRateMs = new(
        "ClusterTopologyRefreshRateMs",
        "30000",
        "Cluster topology refresh rate in millis. The cached topology for the cluster will be invalidated after the specified time, after which it will be updated during the next interaction with the connection.");

    public static readonly AwsWrapperProperty ClusterInstanceHostPattern = new(
        "ClusterInstanceHostPattern",
        null,
        "The cluster instance DNS pattern that will be used to build a complete instance endpoint. A \"?\" character in this pattern should be used as a placeholder for cluster instance names. This pattern is required to be specified for IP address or custom domain connections to AWS RDS clusters. Otherwise, if unspecified, the pattern will be automatically created for AWS RDS clusters.");

    public static readonly AwsWrapperProperty ClusterId = new(
        "ClusterId",
        string.Empty,
        "A unique identifier for the cluster. Connections with the same cluster id share a cluster topology cache. If unspecified, a cluster id is automatically created for AWS RDS clusters.");

    public static readonly AwsWrapperProperty SecretsManagerSecretId = new(
        "secretsManagerSecretId", null, "The name or the ARN of the secret to retrieve.");

    public static readonly AwsWrapperProperty SecretsManagerRegion = new(
        "secretsManagerRegion", "us-east-1", "The region of the secret to retrieve.");

    public static readonly AwsWrapperProperty SecretsManagerExpirationSecs = new(
        "secretsManagerExpirationSec", "870", "The time in seconds that secrets are cached for.");

    public static readonly AwsWrapperProperty SecretsManagerEndpoint = new(
        "secretsManagerEndpoint", null, "The endpoint of the secret to retrieve.");

    public static readonly AwsWrapperProperty IdpEndpoint = new(
        "idpEndpoint", null, "The hosting URL of the Identity Provider");

    public static readonly AwsWrapperProperty IdpPort = new(
      "idpPort", "443", "The hosting port of Identity Provider");

    public static readonly AwsWrapperProperty IdpUsername = new(
        "idpUsername", null, "The federated user name");

    public static readonly AwsWrapperProperty IdpPassword = new(
        "idpPassword", null, "The federated user password");

    public static readonly AwsWrapperProperty RelayingPartyId = new(
        "rpIdentifier", "urn:amazon:webservices", "The relaying party identifier");

    public static readonly AwsWrapperProperty DbUser = new(
        "dbUser", null, "The database user used to access the database");

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
        ClusterTopologyRefreshRateMs,
        ClusterInstanceHostPattern,
        ClusterId
    ];

    public static string GetConnectionUrl(Dictionary<string, string> props)
    {
        return Server.GetString(props) ?? Host.GetString(props) ?? throw new ArgumentException("Connection url is missing.");
    }
}
