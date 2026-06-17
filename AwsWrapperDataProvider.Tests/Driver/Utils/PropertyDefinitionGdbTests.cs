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

using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Tests.Driver.Utils;

/// <summary>
/// Unit tests for GDB property definitions and plugin codes.
/// </summary>
public class PropertyDefinitionGdbTests
{
    // --- Property key and default value tests ---

    [Theory]
    [Trait("Category", "Unit")]
    [MemberData(nameof(GdbPropertyTestData))]
    public void GdbProperty_ShouldHaveCorrectKeyAndDefault(
        AwsWrapperProperty property, string expectedKey, string? expectedDefault)
    {
        Assert.Equal(expectedKey, property.Name);
        Assert.Equal(expectedDefault, property.DefaultValue);
    }

    public static IEnumerable<object?[]> GdbPropertyTestData()
    {
        yield return new object?[]
        {
            PropertyDefinition.GlobalClusterInstanceHostPatterns,
            "GlobalClusterInstanceHostPatterns", null,
        };
        yield return new object?[]
        {
            PropertyDefinition.FailoverHomeRegion,
            "FailoverHomeRegion", null,
        };
        yield return new object?[]
        {
            PropertyDefinition.ActiveHomeFailoverMode,
            "ActiveHomeFailoverMode", null,
        };
        yield return new object?[]
        {
            PropertyDefinition.InactiveHomeFailoverMode,
            "InactiveHomeFailoverMode", null,
        };
        yield return new object?[]
        {
            PropertyDefinition.GdbEnableGlobalWriteForwarding,
            "GdbEnableGlobalWriteForwarding", "false",
        };
    }

    // GDB properties are in InternalWrapperProperties

    [Theory]
    [Trait("Category", "Unit")]
    [MemberData(nameof(InternalWrapperPropertyTestData))]
    public void InternalWrapperProperties_ShouldContainGdbProperty(AwsWrapperProperty property)
    {
        Assert.Contains(property, PropertyDefinition.InternalWrapperProperties);
    }

    public static IEnumerable<object[]> InternalWrapperPropertyTestData()
    {
        yield return new object[] { PropertyDefinition.GlobalClusterInstanceHostPatterns };
        yield return new object[] { PropertyDefinition.FailoverHomeRegion };
        yield return new object[] { PropertyDefinition.ActiveHomeFailoverMode };
        yield return new object[] { PropertyDefinition.InactiveHomeFailoverMode };
        yield return new object[] { PropertyDefinition.GdbEnableGlobalWriteForwarding };
    }

    // Plugin codes

    [Fact]
    [Trait("Category", "Unit")]
    public void PluginCodes_GdbFailover_ShouldBeCorrectValue()
    {
        Assert.Equal("gdbFailover", PluginCodes.GdbFailover);
    }
}
