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

namespace AwsWrapperDataProvider.Driver.Plugins.ReadWriteSplitting;

/// <summary>
/// Factory for creating ReadWriteSplittingPlugin instances.
/// </summary>
public class ReadWriteSplittingPluginFactory : IConnectionPluginFactory
{
    /// <summary>
    /// Creates a new instance of the ReadWriteSplittingPlugin.
    /// </summary>
    /// <param name="pluginService">The plugin service.</param>
    /// <param name="props">Connection properties.</param>
    /// <returns>A new ReadWriteSplittingPlugin instance.</returns>
    public IConnectionPlugin GetInstance(IPluginService pluginService, Dictionary<string, string> props)
    {
        return new ReadWriteSplittingPlugin(pluginService, props);
    }
}
