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
/// Configuration profile for AWS Wrapper connections.
/// </summary>
public class ConfigurationProfile
{
    // TODO : allow use of suppliers

    private readonly string _name;
    private readonly List<IConnectionPluginFactory>? _pluginFactories;
    private readonly Dictionary<string, string>? _properties;

    private IDialect? _dialect;
    private ITargetConnectionDialect? _targetDriverDialect;
    private IConnectionProvider? _connectionProvider;

    public ConfigurationProfile(
        string name,
        List<IConnectionPluginFactory>? pluginFactories,
        Dictionary<string, string>? properties,
        IDialect? dialect,
        ITargetConnectionDialect? targetDriverDialect,
        IConnectionProvider? connectionProvider)
    {
        _name = name;
        _pluginFactories = pluginFactories;
        _properties = properties;
        _dialect = dialect;
        _targetDriverDialect = targetDriverDialect;
        _connectionProvider = connectionProvider;
    }

    /// <summary>
    /// Gets the name of the configuration profile.
    /// </summary>
    public string Name => _name;

    /// <summary>
    /// Gets the properties of the configuration profile.
    /// </summary>
    public Dictionary<string, string>? Properties => _properties;

    /// <summary>
    /// Gets the plugin factories of the configuration profile.
    /// </summary>
    public List<IConnectionPluginFactory>? PluginFactories => _pluginFactories;

    /// <summary>
    /// Gets the dialect of the configuration profile.
    /// </summary>
    public IDialect? Dialect => _dialect ?? null;


    /// <summary>
    /// Gets the target driver dialect of the configuration profile.
    /// </summary>
    public ITargetConnectionDialect? TargetDriverDialect => _targetDriverDialect ?? null;

    /// <summary>
    /// Gets the connection provider of the configuration profile.
    /// </summary>
    public IConnectionProvider? ConnectionProvider => _connectionProvider ?? null;
}
