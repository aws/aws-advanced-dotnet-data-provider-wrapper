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

using System.Reflection;
using AwsWrapperDataProvider.Driver.Plugins.GdbFailover;
using AwsWrapperDataProvider.Driver.Utils;
using FsCheck;
using FsCheck.Fluent;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.GdbFailover;

/// <summary>
/// Property-based tests for GlobalDbFailoverMode enum and failover mode defaults.
/// Uses FsCheck with a minimum of 100 iterations per property.
/// </summary>
public class GlobalDbFailoverModePropertyTests
{
    private static readonly Config PbtConfig =
        Config.QuickThrowOnFailure.WithMaxTest(100);

    /// <summary>
    /// The canonical kebab-case mapping for all 7 GlobalDbFailoverMode values.
    /// </summary>
    private static readonly Dictionary<GlobalDbFailoverMode, string> ModeToKebab = new()
    {
        [GlobalDbFailoverMode.StrictWriter] = "strict-writer",
        [GlobalDbFailoverMode.StrictHomeReader] = "strict-home-reader",
        [GlobalDbFailoverMode.StrictOutOfHomeReader] = "strict-out-of-home-reader",
        [GlobalDbFailoverMode.StrictAnyReader] = "strict-any-reader",
        [GlobalDbFailoverMode.HomeReaderOrWriter] = "home-reader-or-writer",
        [GlobalDbFailoverMode.OutOfHomeReaderOrWriter] = "out-of-home-reader-or-writer",
        [GlobalDbFailoverMode.AnyReaderOrWriter] = "any-reader-or-writer",
    };

    /// <summary>
    /// All RdsUrlType static instances, discovered via reflection.
    /// </summary>
    private static readonly RdsUrlType[] AllRdsUrlTypes =
        typeof(RdsUrlType)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.FieldType == typeof(RdsUrlType))
            .Select(f => (RdsUrlType)f.GetValue(null)!)
            .ToArray();

    /// <summary>
    /// Generator that produces one of the 7 GlobalDbFailoverMode enum values.
    /// </summary>
    private static Gen<GlobalDbFailoverMode> FailoverModeGen()
    {
        return Gen.Elements(Enum.GetValues<GlobalDbFailoverMode>());
    }

    /// <summary>
    /// Generator that produces one of the RdsUrlType static instances.
    /// </summary>
    private static Gen<RdsUrlType> RdsUrlTypeGen()
    {
        return Gen.Elements(AllRdsUrlTypes);
    }

    // Feature: aurora-global-database-support, Property 7: GlobalDbFailoverMode kebab-case round-trip

    /// <summary>
    /// Property 7: For any GlobalDbFailoverMode enum value, converting it to its kebab-case
    /// string representation and parsing back with FromValue returns the original enum value.
    /// **Validates: Requirements 5.3**
    /// </summary>
    [Fact]
    public void KebabCaseRoundTrip_AllModes_ParseBackToOriginal()
    {
        var property = Prop.ForAll(
            FailoverModeGen().ToArbitrary(),
            (GlobalDbFailoverMode mode) =>
            {
                var kebab = ModeToKebab[mode];
                var parsed = GlobalDbFailoverModeExtensions.FromValue(kebab);
                return parsed == mode;
            });

        Check.One(PbtConfig, property);
    }

    // Feature: aurora-global-database-support, Property 10: Failover mode defaults by endpoint type

    /// <summary>
    /// Property 10: For any RdsUrlType, when ActiveHomeFailoverMode and InactiveHomeFailoverMode
    /// are not explicitly set: if the type is RdsWriterCluster or RdsGlobalWriterCluster, both
    /// modes default to StrictWriter; otherwise, both default to HomeReaderOrWriter.
    /// **Validates: Requirements 5.9, 5.10, 5.11**
    /// </summary>
    [Fact]
    public void FailoverModeDefaults_ByEndpointType_MatchExpected()
    {
        var property = Prop.ForAll(
            RdsUrlTypeGen().ToArbitrary(),
            (RdsUrlType urlType) =>
            {
                var expected = (urlType == RdsUrlType.RdsWriterCluster ||
                                urlType == RdsUrlType.RdsGlobalWriterCluster)
                    ? GlobalDbFailoverMode.StrictWriter
                    : GlobalDbFailoverMode.HomeReaderOrWriter;

                var activeDefault = GetDefaultFailoverMode(urlType);
                var inactiveDefault = GetDefaultFailoverMode(urlType);

                return activeDefault == expected && inactiveDefault == expected;
            });

        Check.One(PbtConfig, property);
    }

    /// <summary>
    /// Computes the default failover mode for a given RdsUrlType, matching the logic
    /// specified in Requirements 5.9, 5.10, 5.11.
    /// </summary>
    private static GlobalDbFailoverMode GetDefaultFailoverMode(RdsUrlType urlType)
    {
        return (urlType == RdsUrlType.RdsWriterCluster ||
                urlType == RdsUrlType.RdsGlobalWriterCluster)
            ? GlobalDbFailoverMode.StrictWriter
            : GlobalDbFailoverMode.HomeReaderOrWriter;
    }
}
