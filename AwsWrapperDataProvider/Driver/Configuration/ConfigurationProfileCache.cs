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

using System.Collections.Concurrent;

namespace AwsWrapperDataProvider.Driver.Configuration;

/// <summary>
/// Static class for managing configuration profiles.
/// </summary>
public static class ConfigurationProfileCache
{
    private static readonly ConcurrentDictionary<string, ConfigurationProfile> Profiles = new();

    /// <summary>
    /// Adds or replaces a configuration profile.
    /// </summary>
    /// <param name="name">The name of the profile.</param>
    /// <param name="profile">The configuration profile.</param>
    public static void AddOrReplaceProfile(string name, ConfigurationProfile profile)
    {
        Profiles[name] = profile;
    }

    /// <summary>
    /// Gets a configuration profile by name.
    /// </summary>
    /// <param name="name">The name of the profile.</param>
    /// <returns>The configuration profile, or null if not found.</returns>
    public static ConfigurationProfile? GetProfileConfiguration(string name)
    {
        return Profiles.TryGetValue(name, out var profile) ? profile : null;
    }
}
