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

using System.Globalization;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace AwsWrapperDataProvider.Tests;

/// <summary>
/// Reports and enforces which .NET runtime is hosting the suite.
/// </summary>
/// <remarks>
/// The test platform labels a run with the framework the assembly was compiled for (for example
/// "(net8.0|x64)"), which is not the same as the runtime that loaded it: a net8.0 assembly runs happily
/// on a .NET 10 runtime whenever roll-forward allows it. Since the suite is run once per framework
/// specifically to exercise two runtimes, a silent roll-forward would make one of those passes
/// worthless while still reporting success.
/// </remarks>
public class TestRuntimeTests
{
    private readonly ITestOutputHelper output;

    public TestRuntimeTests(ITestOutputHelper output)
    {
        this.output = output;
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RunningRuntimeMajorVersion_MatchesCompiledTargetFramework()
    {
        var compiledFor = typeof(TestRuntimeTests).Assembly
            .GetCustomAttribute<TargetFrameworkAttribute>()?.FrameworkName;

        this.output.WriteLine(
            $"compiled for: {compiledFor} | running on: {RuntimeInformation.FrameworkDescription} "
            + $"| Environment.Version: {Environment.Version} | RID: {RuntimeInformation.RuntimeIdentifier}");

        Assert.NotNull(compiledFor);

        // FrameworkName looks like ".NETCoreApp,Version=v8.0"; the major is what has to line up with
        // the runtime, since a patch or minor difference is expected and permitted by roll-forward.
        var compiledMajor = int.Parse(
            compiledFor!.Split("Version=v")[1].Split('.')[0],
            CultureInfo.InvariantCulture);

        Assert.Equal(compiledMajor, Environment.Version.Major);
    }
}
