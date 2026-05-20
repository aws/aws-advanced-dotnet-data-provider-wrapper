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

using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Plugins.AuroraStaleDns;
using AwsWrapperDataProvider.Driver.Utils.Telemetry;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins;

/// <summary>
/// Unit tests for <see cref="AuroraStaleDnsHelper"/>'s telemetry wiring.
///
/// <para>This file covers the constructor-level surface only: that the
/// <c>staleDNS.stale.detected</c> counter is created once, with the expected
/// name, via the plugin service's telemetry factory.</para>
///
/// <para>The increment-path test (verifying that
/// <see cref="AuroraStaleDnsHelper.OpenVerifiedConnectionAsync"/> increments
/// the counter exactly when stale DNS is detected) is deferred — see
/// <c>.kiro/specs/wrapper-telemetry/deferred-decisions.md</c>. The stale-DNS
/// branch depends on the private static
/// <c>AuroraStaleDnsHelper.GetHostIpAddress</c> method performing real
/// <c>System.Net.Dns.GetHostAddresses</c> calls, which is awkward to drive
/// from a unit test without a refactor to introduce a DNS-resolution seam.</para>
/// </summary>
public class AuroraStaleDnsHelperTelemetryTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void Constructor_CreatesStaleDnsDetectedCounter()
    {
        Mock<ITelemetryFactory> mockFactory = new();
        Mock<ITelemetryCounter> mockCounter = new();
        mockFactory
            .Setup(f => f.CreateCounter(It.IsAny<string>()))
            .Returns(mockCounter.Object);

        Mock<IPluginService> mockPluginService = new();
        mockPluginService.Setup(s => s.TelemetryFactory).Returns(mockFactory.Object);

        _ = new AuroraStaleDnsHelper(mockPluginService.Object);

        // Exactly one counter is created in the constructor with the
        // "staleDNS.stale.detected" name.
        mockFactory.Verify(f => f.CreateCounter("staleDNS.stale.detected"), Times.Once);
        mockFactory.Verify(f => f.CreateCounter(It.IsAny<string>()), Times.Once);
    }
}
