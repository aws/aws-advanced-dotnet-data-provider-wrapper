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
using AwsWrapperDataProvider.Driver.HostInfo;

namespace AwsWrapperDataProvider.Authentication;

/// <summary>
/// Selects the <see cref="AWSCredentials"/> used for the AWS API calls the wrapper makes on a
/// consumer's behalf (RDS IAM token generation, Secrets Manager, and RDS control-plane calls such as
/// custom-endpoint discovery). Registered with the <see cref="AwsCredentialsManager"/> via
/// <see cref="AwsCredentialsManager.SetCustomHandler"/>.
/// <para>
/// Return <c>null</c> to fall back to the AWS SDK default credentials chain. The returned
/// <see cref="AWSCredentials"/> is itself the refreshable unit — for example
/// <c>AssumeRoleAWSCredentials</c> refreshes internally when the SDK resolves it — so the handler
/// should be a cheap, non-blocking selector and must not perform network I/O itself. Handlers are
/// invoked once per credential resolution and may be called concurrently, so they must be
/// thread-safe.
/// </para>
/// </summary>
/// <param name="hostSpec">The endpoint the credentials will be used against; may be <c>null</c> when the credentials are not tied to a specific host.</param>
/// <param name="props">A read-only view of the connection properties.</param>
/// <returns>The credentials to use, or <c>null</c> to use the AWS SDK default credentials chain.</returns>
public delegate AWSCredentials? AwsCredentialsProviderHandler(
    HostSpec? hostSpec,
    IReadOnlyDictionary<string, string> props);
