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

using AwsWrapperDataProvider.Driver.Utils;
using Xunit;

namespace AwsWrapperDataProvider.Tests.Driver.Utils;

public class AwsWrapperPropertyTests
{
    [Theory]
    [InlineData("TestProperty", "DefaultValue", "Test description", true, new[] { "Option1", "Option2" })]
    [InlineData("Port", "3306", "Port number", false, null)]
    [InlineData("Host", "localhost", "Database host", true, null)]
    public void Constructor_SetsPropertiesCorrectly(
        string name, 
        string defaultValue, 
        string description, 
        bool required, 
        string[] choices)
    {
        var property = new AwsWrapperProperty(name, defaultValue, description, required, choices);
        
        Assert.Equal(name, property.Name);
        Assert.Equal(defaultValue, property.DefaultValue);
        Assert.Equal(description, property.Description);
        Assert.Equal(required, property.Required);
        
        if (choices != null)
        {
            Assert.Equal(choices.Length, property.Choices?.Length);
            for (int i = 0; i < choices.Length; i++)
            {
                Assert.Equal(choices[i], property.Choices?[i]);
            }
        }
        else
        {
            Assert.Null(property.Choices);
        }
    }
    
    [Theory]
    [InlineData("TestProperty", "DefaultValue", "ActualValue", "ActualValue")]
    [InlineData("TestProperty", "DefaultValue", null, "DefaultValue")]
    [InlineData("TestProperty", null, null, null)]
    public void GetString_ReturnsExpectedValue(
        string propertyName, 
        string defaultValue, 
        string propertyValue, 
        string expectedResult)
    {
        var property = new AwsWrapperProperty(propertyName, defaultValue, "Test description");
        var properties = new Dictionary<string, string>();
        
        if (propertyValue != null)
        {
            properties[propertyName] = propertyValue;
        }
        
        var result = property.GetString(properties);
        
        Assert.Equal(expectedResult, result);
    }
    
    [Theory]
    [InlineData("Port", "5432", "3306", 3306)]
    [InlineData("Port", "5432", null, 5432)]
    [InlineData("Port", null, null, null)]
    public void GetInt_ReturnsExpectedValue(
        string propertyName, 
        string defaultValue, 
        string propertyValue, 
        int? expectedResult)
    {
        var property = new AwsWrapperProperty(propertyName, defaultValue, "Test description");
        var properties = new Dictionary<string, string>();
        
        if (propertyValue != null)
        {
            properties[propertyName] = propertyValue;
        }
        
        var result = property.GetInt(properties);
        
        Assert.Equal(expectedResult, result);
    }
    
    [Fact]
    public void GetInt_WithInvalidValue_ThrowsFormatException()
    {
        var property = new AwsWrapperProperty("Port", "5432", "Test description");
        var properties = new Dictionary<string, string>
        {
            { "Port", "invalid" }
        };
        
        Assert.Throws<FormatException>(() => property.GetInt(properties));
    }
    
    [Theory]
    [InlineData("TestProperty", "NewValue", true)]
    [InlineData("TestProperty", null, false)]
    public void Set_ModifiesDictionaryCorrectly(
        string propertyName, 
        string valueToSet, 
        bool shouldExistAfterSet)
    {
        var property = new AwsWrapperProperty(propertyName, "DefaultValue", "Test description");
        var properties = new Dictionary<string, string>
        {
            { propertyName, "ExistingValue" }
        };
        
        property.Set(properties, valueToSet);
        
        if (shouldExistAfterSet)
        {
            Assert.True(properties.ContainsKey(propertyName));
            Assert.Equal(valueToSet, properties[propertyName]);
        }
        else
        {
            Assert.False(properties.ContainsKey(propertyName));
        }
    }
    
    [Fact]
    public void Set_WithNewKey_AddsKeyValue()
    {
        var property = new AwsWrapperProperty("NewProperty", "DefaultValue", "Test description");
        var properties = new Dictionary<string, string>();
        
        property.Set(properties, "NewValue");
        
        Assert.Single(properties);
        Assert.Equal("NewValue", properties["NewProperty"]);
    }
}
