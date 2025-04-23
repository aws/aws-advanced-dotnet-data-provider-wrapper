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

namespace AwsWrapperDataProvider.Driver.Utils;

public class AwsWrapperProperty
{
    public readonly string? DefaultValue;

    public string Name;

    public string? Description = null;

    public bool Required = false;

    public string? Value = null;

    public string[]? Choices = null;

    public AwsWrapperProperty(
        string name,
        string? defaultValue,
        string description,
        bool required = false,
        string[]? choices = null)
    {
        this.Name = name;
        this.DefaultValue = defaultValue;
        this.Description = description;
        this.Required = required;
        this.Choices = choices;
    }

    public string? GetString(Dictionary<string, string> properties)
    {
        return properties.TryGetValue(this.Name, out string? value) ? value : this.DefaultValue;
    }

    public void Set(Dictionary<string, string> props, string? value)
    {
        if (value == null)
        {
            props.Remove(this.Name);
        }
        else
        {
            props[this.Name] = value;
        }
    }
}
