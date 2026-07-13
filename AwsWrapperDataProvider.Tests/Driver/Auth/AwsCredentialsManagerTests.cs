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

using Amazon.Runtime;
using AwsWrapperDataProvider.Authentication;
using AwsWrapperDataProvider.Driver.HostInfo;

namespace AwsWrapperDataProvider.Tests.Driver.Auth;

public class AwsCredentialsManagerTests : IDisposable
{
    private static readonly IReadOnlyDictionary<string, string> EmptyProps = new Dictionary<string, string>();

    private readonly HostSpec hostA = new("a.host.us-east-1.rds.amazonaws.com", 5432, HostRole.Writer, HostAvailability.Available);
    private readonly HostSpec hostB = new("b.host.us-east-1.rds.amazonaws.com", 5432, HostRole.Writer, HostAvailability.Available);

    public AwsCredentialsManagerTests()
    {
        AwsCredentialsManager.ResetCustomHandler();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetCredentials_WithNoHandler_ReturnsNull()
    {
        Assert.Null(AwsCredentialsManager.GetCredentials(this.hostA, EmptyProps));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SetCustomHandler_ReturnsHandlerCredentials()
    {
        var credentials = new BasicAWSCredentials("access", "secret");
        AwsCredentialsManager.SetCustomHandler((_, _) => credentials);

        Assert.Same(credentials, AwsCredentialsManager.GetCredentials(this.hostA, EmptyProps));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SetCustomHandler_SelectsCredentialsPerHost()
    {
        var credentialsA = new BasicAWSCredentials("access-a", "secret-a");
        var credentialsB = new BasicAWSCredentials("access-b", "secret-b");

        AwsCredentialsManager.SetCustomHandler((hostSpec, _) =>
            hostSpec?.Host == this.hostA.Host ? credentialsA : credentialsB);

        Assert.Same(credentialsA, AwsCredentialsManager.GetCredentials(this.hostA, EmptyProps));
        Assert.Same(credentialsB, AwsCredentialsManager.GetCredentials(this.hostB, EmptyProps));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetCredentials_PassesHostSpecAndPropsToHandler()
    {
        HostSpec? observedHost = null;
        IReadOnlyDictionary<string, string>? observedProps = null;
        var props = new Dictionary<string, string> { ["key"] = "value" };

        AwsCredentialsManager.SetCustomHandler((hostSpec, p) =>
        {
            observedHost = hostSpec;
            observedProps = p;
            return null;
        });

        _ = AwsCredentialsManager.GetCredentials(this.hostA, props);

        Assert.Same(this.hostA, observedHost);
        Assert.Same(props, observedProps);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetCredentials_HandlerReturnsNull_ReturnsNull()
    {
        AwsCredentialsManager.SetCustomHandler((_, _) => null);

        Assert.Null(AwsCredentialsManager.GetCredentials(this.hostA, EmptyProps));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SetCustomHandler_LastRegistrationWins()
    {
        var first = new BasicAWSCredentials("first", "secret");
        var second = new BasicAWSCredentials("second", "secret");

        AwsCredentialsManager.SetCustomHandler((_, _) => first);
        AwsCredentialsManager.SetCustomHandler((_, _) => second);

        Assert.Same(second, AwsCredentialsManager.GetCredentials(this.hostA, EmptyProps));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ResetCustomHandler_RevertsToNull()
    {
        AwsCredentialsManager.SetCustomHandler((_, _) => new BasicAWSCredentials("access", "secret"));
        AwsCredentialsManager.ResetCustomHandler();

        Assert.Null(AwsCredentialsManager.GetCredentials(this.hostA, EmptyProps));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void SetCustomHandler_WithNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => AwsCredentialsManager.SetCustomHandler(null!));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetCredentials_WithNullHostSpec_IsSupported()
    {
        var credentials = new BasicAWSCredentials("access", "secret");
        AwsCredentialsManager.SetCustomHandler((_, _) => credentials);

        Assert.Same(credentials, AwsCredentialsManager.GetCredentials(null, EmptyProps));
    }

    public void Dispose()
    {
        AwsCredentialsManager.ResetCustomHandler();
        GC.SuppressFinalize(this);
    }
}
