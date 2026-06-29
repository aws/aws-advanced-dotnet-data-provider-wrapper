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

using AwsWrapperDataProvider.Driver.Auth;

namespace AwsWrapperDataProvider.Tests.Driver.Auth;

public class PasswordProviderRegistryTests
{
    private static PasswordProviderRegistration Registration(string token) =>
        new(_ => new ValueTask<string>(token));

    [Fact]
    [Trait("Category", "Unit")]
    public async Task RegisterAndTryGet_ReturnsRegisteredProvider()
    {
        PasswordProviderRegistry.Clear();
        PasswordProviderRegistry.Register("key-1", Registration("token-1"));

        Assert.True(PasswordProviderRegistry.TryGet("key-1", out var registration));
        Assert.NotNull(registration);
        Assert.Equal("token-1", await registration!.Provider(CancellationToken.None));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void TryGet_Miss_ReturnsFalse()
    {
        PasswordProviderRegistry.Clear();
        Assert.False(PasswordProviderRegistry.TryGet("absent", out var registration));
        Assert.Null(registration);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Register_Overwrites_ExistingRegistration()
    {
        PasswordProviderRegistry.Clear();
        PasswordProviderRegistry.Register("key-1", Registration("old"));
        PasswordProviderRegistry.Register("key-1", Registration("new"));

        Assert.True(PasswordProviderRegistry.TryGet("key-1", out var registration));
        Assert.Equal("new", await registration!.Provider(CancellationToken.None));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Remove_DeletesRegistration()
    {
        PasswordProviderRegistry.Clear();
        PasswordProviderRegistry.Register("key-1", Registration("token-1"));
        PasswordProviderRegistry.Remove("key-1");

        Assert.False(PasswordProviderRegistry.TryGet("key-1", out _));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ProviderKeyPropertyName_IsReservedInternalHandle()
    {
        // The provider key is a reserved "__"-prefixed handle, not a user-facing PropertyDefinition.
        Assert.Equal("__awsWrapperPasswordProviderKey", PasswordProviderRegistry.ProviderKeyPropertyName);
        Assert.StartsWith("__", PasswordProviderRegistry.ProviderKeyPropertyName);
    }
}
