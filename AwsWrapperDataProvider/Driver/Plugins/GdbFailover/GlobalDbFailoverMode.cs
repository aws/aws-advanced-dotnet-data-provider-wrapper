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

namespace AwsWrapperDataProvider.Driver.Plugins.GdbFailover;

/// <summary>
/// Enumeration of global database failover modes that determine the behavior
/// during failover scenarios in Aurora Global Databases.
/// </summary>
public enum GlobalDbFailoverMode
{
    StrictWriter,
    StrictHomeReader,
    StrictOutOfHomeReader,
    StrictAnyReader,
    HomeReaderOrWriter,
    OutOfHomeReaderOrWriter,
    AnyReaderOrWriter,
}

/// <summary>
/// Extension methods for parsing <see cref="GlobalDbFailoverMode"/> from string values.
/// </summary>
public static class GlobalDbFailoverModeExtensions
{
    private static readonly Dictionary<string, GlobalDbFailoverMode> NameToValue =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["strict-writer"] = GlobalDbFailoverMode.StrictWriter,
            ["strict-home-reader"] = GlobalDbFailoverMode.StrictHomeReader,
            ["strict-out-of-home-reader"] = GlobalDbFailoverMode.StrictOutOfHomeReader,
            ["strict-any-reader"] = GlobalDbFailoverMode.StrictAnyReader,
            ["home-reader-or-writer"] = GlobalDbFailoverMode.HomeReaderOrWriter,
            ["out-of-home-reader-or-writer"] = GlobalDbFailoverMode.OutOfHomeReaderOrWriter,
            ["any-reader-or-writer"] = GlobalDbFailoverMode.AnyReaderOrWriter,
        };

    /// <summary>
    /// Parses a kebab-case string value into a <see cref="GlobalDbFailoverMode"/>.
    /// </summary>
    /// <param name="value">The kebab-case string to parse (e.g., "strict-writer"), or null.</param>
    /// <returns>The corresponding <see cref="GlobalDbFailoverMode"/>, or null if the input is null.</returns>
    /// <exception cref="ArgumentException">Thrown when the value does not match any known failover mode.</exception>
    public static GlobalDbFailoverMode? FromValue(string? value)
    {
        if (value == null)
        {
            return null;
        }

        if (NameToValue.TryGetValue(value, out var mode))
        {
            return mode;
        }

        throw new ArgumentException($"Invalid GlobalDbFailoverMode value: {value}");
    }
}
