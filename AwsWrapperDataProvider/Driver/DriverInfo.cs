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

namespace AwsWrapperDataProvider.Driver;

/// <summary>
/// How this driver identifies itself to RDS.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Version"/> is the one place the release version is written down, and it has to be bumped
/// for each release. It is not derived from the assembly version: the release workflow supplies the
/// version to <c>dotnet pack</c> only, building beforehand with <c>--no-build</c>, so the compiled
/// assembly carries the default version rather than the release one.
/// </para>
/// <para>
/// RDS reads these values from the argument passed to <c>rds_tools.show_topology</c> and
/// <c>pg_catalog.get_blue_green_fast_switchover_metadata</c>, so the format is a wire contract with the
/// service and not a free-form label.
/// </para>
/// </remarks>
internal static class DriverInfo
{
    /// <summary>
    /// Driver name reported to RDS.
    /// </summary>
    internal const string Name = "aws_advanced_dotnet_data_provider_wrapper";

    /// <summary>
    /// Release version reported to RDS. Bump this for every release.
    /// </summary>
    internal const string Version = "2.2.0";

    /// <summary>
    /// The <c>name-version</c> form RDS expects from the topology and Blue/Green metadata functions.
    /// </summary>
    internal const string NameAndVersion = $"{Name}-{Version}";
}
