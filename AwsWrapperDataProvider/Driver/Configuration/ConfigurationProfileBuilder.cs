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

using AwsWrapperDataProvider.Driver.ConnectionProviders;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.TargetConnectionDialects;

namespace AwsWrapperDataProvider.Driver.Configuration;

/// <summary>
/// Builder for configuration profiles.
/// </summary>
public class ConfigurationProfileBuilder
{
    private string? _name;
    private List<IConnectionPluginFactory>? _pluginFactories;
    private Dictionary<string, string>? _properties;
    private IDialect? _dialect;
    private ITargetConnectionDialect? _targetConnectionDialect;
    private IConnectionProvider? _connectionProvider;

    private ConfigurationProfileBuilder() { }

    /// <summary>
    /// Gets a new instance of the builder.
    /// </summary>
    /// <returns>A new builder instance.</returns>
    public static ConfigurationProfileBuilder Get() => new();

    /// <summary>
    /// Sets the name of the configuration profile.
    /// </summary>
    /// <param name="name">The name of the configuration profile.</param>
    /// <returns>This builder instance.</returns>
    public ConfigurationProfileBuilder WithName(string name)
    {
        this._name = name;
        return this;
    }

    /// <summary>
    /// Sets the properties of the configuration profile.
    /// </summary>
    /// <param name="properties">The properties of the configuration profile.</param>
    /// <returns>This builder instance.</returns>
    public ConfigurationProfileBuilder WithProperties(Dictionary<string, string>? properties)
    {
        this._properties = properties;
        return this;
    }

    /// <summary>
    /// Sets the plugin factories of the configuration profile.
    /// </summary>
    /// <param name="pluginFactories">The plugin factories of the configuration profile.</param>
    /// <returns>This builder instance.</returns>
    public ConfigurationProfileBuilder WithPluginFactories(List<IConnectionPluginFactory>? pluginFactories)
    {
        this._pluginFactories = pluginFactories;
        return this;
    }

    /// <summary>
    /// Sets the dialect of the configuration profile.
    /// </summary>
    /// <param name="dialect">The dialect of the configuration profile.</param>
    /// <returns>This builder instance.</returns>
    public ConfigurationProfileBuilder WithDialect(IDialect? dialect)
    {
        this._dialect = dialect;
        return this;
    }

    /// <summary>
    /// Sets the target driver dialect of the configuration profile.
    /// </summary>
    /// <param name="targetConnectionDialect">The target driver dialect of the configuration profile.</param>
    /// <returns>This builder instance.</returns>
    public ConfigurationProfileBuilder WithTargetConnectionDialect(ITargetConnectionDialect? targetConnectionDialect)
    {
        this._targetConnectionDialect = targetConnectionDialect;
        return this;
    }

    /// <summary>
    /// Sets the connection provider of the configuration profile.
    /// </summary>
    /// <param name="connectionProvider">The connection provider of the configuration profile.</param>
    /// <returns>This builder instance.</returns>
    public ConfigurationProfileBuilder WithConnectionProvider(IConnectionProvider? connectionProvider)
    {
        this._connectionProvider = connectionProvider;
        return this;
    }

    /// <summary>
    /// Copies settings from an existing profile.
    /// </summary>
    /// <param name="presetProfileName">The name of the preset profile to copy from.</param>
    /// <returns>This builder instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the preset profile is not found.</exception>
    public ConfigurationProfileBuilder From(string presetProfileName)
    {
        var configurationProfile = ConfigurationProfileCache.GetProfileConfiguration(presetProfileName);

        if (configurationProfile == null)
        {
            throw new InvalidOperationException($"Configuration profile '{presetProfileName}' not found.");
        }

        this._pluginFactories = configurationProfile.PluginFactories;
        this._properties = configurationProfile.Properties;
        this._dialect = configurationProfile.Dialect;
        this._targetConnectionDialect = configurationProfile.TargetConnectionDialect;
        this._connectionProvider = configurationProfile.ConnectionProvider;

        return this;
    }

    /// <summary>
    /// Builds the configuration profile.
    /// </summary>
    /// <returns>The built configuration profile.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the profile name is null or empty, or when trying to modify a built-in preset.</exception>
    public ConfigurationProfile Build()
    {
        if (string.IsNullOrEmpty(this._name))
        {
            throw new InvalidOperationException("Profile name is required.");
        }

        return new ConfigurationProfile(
            this._name,
            this._pluginFactories,
            this._properties,
            this._dialect,
            this._targetConnectionDialect,
            this._connectionProvider);
    }

    /// <summary>
    /// Builds the configuration profile and adds it to the registry.
    /// </summary>
    public void BuildAndSet()
    {
        ConfigurationProfileCache.AddOrReplaceProfile(this._name!, this.Build());
    }
}
