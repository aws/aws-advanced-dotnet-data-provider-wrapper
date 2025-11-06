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

using System.Linq.Expressions;
using System.Reflection;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Moq;

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

    public static void VerifyDelegatesToExecutePipeline<TMock, TReturn>(
        Mock<ConnectionPluginManager> mockPluginManager,
        Mock<TMock> mockObject,
        Expression<Func<TMock, TReturn>> expression)
        where TMock : class
    {
        mockPluginManager.Verify(
            p => p.Execute(
                mockObject.Object,
                It.IsAny<string>(),
                It.IsAny<ADONetDelegate<It.IsAnyType>>(),
                It.IsAny<object[]>()),
            Times.Once);
        mockObject.Verify(expression, Times.Once);
    }

    public static void VerifyDelegatesToTargetObject<TMock, TReturn>(
    Mock<TMock> mockObject,
    Expression<Func<TMock, TReturn>> expression)
    where TMock : class
    {
        mockObject.Verify(expression, Times.Once);
    }

    public static void VerifyDelegatesToExecutePipeline<TMock>(
        Mock<ConnectionPluginManager> mockPluginManager,
        Mock<TMock> mockObject,
        Expression<Action<TMock>> expression)
        where TMock : class
    {
        mockPluginManager.Verify(
            p => p.Execute(
                mockObject.Object,
                It.IsAny<string>(),
                It.IsAny<ADONetDelegate<object>>(),
                It.IsAny<object[]>()),
            Times.Once);
        mockObject.Verify(expression, Times.Once);
    }

    public static void VerifyDelegatesToExecutePipeline<TMock, TReturn>(
        Mock<ConnectionPluginManager> mockPluginManager,
        Mock<TMock> mockObject,
        string methodName)
        where TMock : class
    {
        mockPluginManager.Verify(
            p => p.Execute<TReturn>(
                mockObject.Object,
                methodName,
                It.IsAny<ADONetDelegate<TReturn>>(),
                It.IsAny<object[]>()),
            Times.Once);
    }

    public static string? GetTraitValue(MethodBase method, string traitName)
    {
        var traitAttributes = method.GetCustomAttributes<TraitAttribute>(inherit: true);

        return traitAttributes
            .Where(attr => attr.Name == traitName)
            .Select(attr => attr.Value)
            .FirstOrDefault();
    }

    public static DatabaseEngine GetEngineFromTrait(MethodBase method, string traitName)
    {
        var engineValue = GetTraitValue(method, traitName);
        return engineValue switch
        {
            "mysql" => DatabaseEngine.MYSQL,
            "pg" => DatabaseEngine.PG,
            _ => throw new NotSupportedException($"Unsupported engine type: {engineValue}"),
        };
    }
}
