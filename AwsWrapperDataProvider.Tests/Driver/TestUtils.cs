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

namespace AwsWrapperDataProvider.Tests.Driver;

public static class TestUtils
{
    public static T? GetNonPublicInstanceField<T>(object instance, string fieldName)
    {
        ArgumentNullException.ThrowIfNull(instance);

        var field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
        if (field == null)
        {
            throw new ArgumentException($"Field '{fieldName}' not found.");
        }

        return (T?)field.GetValue(instance);
    }

    public static void SetNonPublicInstanceField<T>(object instance, string fieldName, T value)
    {
        ArgumentNullException.ThrowIfNull(instance);

        var field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
        if (field == null)
        {
            throw new ArgumentException($"Field '{fieldName}' not found.");
        }

        field.SetValue(instance, value);
    }

    public static void SetNonPublicStaticField<T>(Type type, string fieldName, T value)
    {
        ArgumentNullException.ThrowIfNull(type);

        var field = type.GetField(fieldName, BindingFlags.Static | BindingFlags.NonPublic);
        if (field == null)
        {
            throw new ArgumentException($"Field '{fieldName}' not found.");
        }

        field.SetValue(null, value);
    }
}
