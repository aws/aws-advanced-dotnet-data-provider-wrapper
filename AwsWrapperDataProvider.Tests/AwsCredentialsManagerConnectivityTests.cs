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
using Amazon.Runtime;
using Amazon.Runtime.Credentials;
using AwsWrapperDataProvider.Authentication;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Plugin.Iam.Iam;
using AwsWrapperDataProvider.Tests.Container.Utils;

namespace AwsWrapperDataProvider.Tests;

/// <summary>
/// Integration tests proving the <see cref="AwsCredentialsManager"/> custom handler is actually used
/// to generate the RDS IAM authentication token, rather than the AWS SDK default credentials chain.
/// <para>
/// The proof is a distinguishing-behavior triple against a real Aurora cluster:
/// <list type="bullet">
/// <item>Baseline — no handler registered — connects via the default chain (the control).</item>
/// <item>Positive — a handler returning the same valid credentials the default chain would resolve —
/// still connects, and the handler is observably invoked.</item>
/// <item>Negative — a handler returning deliberately invalid credentials — FAILS to connect. Because
/// the RDS token is signed with whatever credentials are supplied, a rejected connection can only mean
/// the custom (invalid) credentials actually flowed through instead of the default chain (which would
/// have succeeded).</item>
/// </list>
/// The negative case is the clincher: if the manager were ignored, the invalid handler would have no
/// effect and the connection would succeed via the default chain.
/// </para>
/// <para>
/// Tests are engine-agnostic: they build the connection string and connection via
/// <see cref="ConnectionStringHelper.GetUrl"/> and <see cref="AuroraTestUtils.CreateAwsWrapperConnection"/>
/// using the environment's configured <c>Engine</c>, so a single test runs against whichever engine
/// (PostgreSQL or MySQL) the test environment provides.
/// </para>
/// </summary>
public class AwsCredentialsManagerConnectivityTests : IntegrationTestBase
{
    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();

        // Tests run serially (DisableTestParallelization) and the base class clears the IAM token cache
        // on dispose, so resetting the handler at the start of each test fully isolates them.
        AwsCredentialsManager.ResetCustomHandler();
        IamAuthPlugin.ClearCache();
    }

    private static string GetIamConnectionString()
    {
        var iamUser = TestEnvironment.Env.Info.IamUsername;
        var iamRegion = TestEnvironment.Env.Info.Region;
        var connectionString = ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, iamUser, null, DefaultDbName);
        return connectionString + $";Plugins=iam;IamRegion={iamRegion}";
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Database", "mysql")]
    [Trait("Engine", "aurora")]
    public async Task Wrapper_WithCustomHandler_UsesHandlerCredentials()
    {
        // Register a handler that returns the same valid credentials the default chain would resolve,
        // and record that it was invoked with the expected endpoint.
        var handlerInvoked = false;
        HostSpec? observedHost = null;
        AWSCredentials validCredentials = DefaultAWSCredentialsIdentityResolver.GetCredentials();
        AwsCredentialsManager.SetCustomHandler((hostSpec, _) =>
        {
            handlerInvoked = true;
            observedHost = hostSpec;
            return validCredentials;
        });

        using DbConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, GetIamConnectionString());

        await AuroraUtils.OpenDbConnection(connection, async: true);
        Assert.Equal(ConnectionState.Open, connection.State);

        // The handler was consulted, and it saw the IAM endpoint (not a null host).
        Assert.True(handlerInvoked, "The custom credentials handler was never invoked.");
        Assert.NotNull(observedHost);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Database", "mysql")]
    [Trait("Engine", "aurora")]
    public async Task Wrapper_WithInvalidHandlerCredentials_FailsToConnect()
    {
        // Return deliberately invalid credentials. The token is signed with these, so if the manager is
        // really used, the token is rejected and the connection fails. (If the manager were ignored, the
        // default chain would sign a valid token and this would succeed — see the baseline test.)
        AwsCredentialsManager.SetCustomHandler((_, _) =>
            new BasicAWSCredentials("AKIAINVALIDEXAMPLEKEY", "invalid-secret-access-key-value-000000000"));

        using DbConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, GetIamConnectionString());

        // The token is signed with the invalid credentials, so the connection must be rejected. If the
        // manager were ignored, the default chain would sign a valid token and this would succeed.
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await AuroraUtils.OpenDbConnection(connection, async: true));
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Database", "mysql")]
    [Trait("Engine", "aurora")]
    public async Task Wrapper_WithNoHandler_ConnectsViaDefaultChain()
    {
        // Control test: with no handler registered, the default credentials chain must still work. This
        // is what makes the invalid-handler test meaningful — the same connection string and endpoint
        // succeed here, so a failure there can only come from the handler's credentials.
        Assert.Null(AwsCredentialsManager.GetCredentials(null, new Dictionary<string, string>()));

        using DbConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, GetIamConnectionString());

        await AuroraUtils.OpenDbConnection(connection, async: true);
        Assert.Equal(ConnectionState.Open, connection.State);
    }
}
