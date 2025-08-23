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

using Toxiproxy.Net;
using Toxiproxy.Net.Toxics;

namespace AwsWrapperDataProvider.Tests.Container.Utils;

public class ProxyHelper
{
    public static async Task EnableAllConnectivityAsync()
    {
        foreach (Proxy proxy in TestEnvironment.Env.Proxies)
        {
            try
            {
                await EnableConnectivityAsync(proxy);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to enable proxy {proxy.Name}: {ex.Message}");
            }
        }
    }

    public static async Task EnableConnectivityAsync(string instanceName)
    {
        Proxy proxy = TestEnvironment.Env.GetProxy(instanceName);
        await EnableConnectivityAsync(proxy);
    }

    private static async Task EnableConnectivityAsync(Proxy proxy)
    {
        try
        {
            var toxics = await proxy.GetAllToxicsAsync();
            foreach (ToxicBase toxic in toxics.Where(t => t.Name == "DOWN-STREAM" || t.Name == "UP-STREAM"))
            {
                try
                {
                    await proxy.RemoveToxicAsync(toxic.Name);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error removing toxic: {ex}");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error enabling connectivity: {ex}");
        }

        Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} Enabled connectivity to {proxy.Name}");
    }

    public static async Task DisableAllConnectivityAsync()
    {
        foreach (Proxy proxy in TestEnvironment.Env.Proxies)
        {
            await DisableConnectivityAsync(proxy);
        }
    }

    public static async Task DisableConnectivityAsync(string instanceName)
    {
        Proxy proxy = TestEnvironment.Env.GetProxy(instanceName);
        await DisableConnectivityAsync(proxy);
    }

    private static async Task DisableConnectivityAsync(Proxy proxy)
    {
        try
        {
            BandwidthToxic bandWidthToxic = new()
            {
                Name = "DOWN-STREAM",
                Stream = ToxicDirection.DownStream, // from database server towards driver
                Toxicity = 1.0f,
            };

            bandWidthToxic.Attributes.Rate = 0;
            await proxy.AddAsync(bandWidthToxic);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error disabling connectivity DOWN-STREAM: {ex}");
        }

        try
        {
            BandwidthToxic bandWidthToxic = new()
            {
                Name = "UP-STREAM",
                Stream = ToxicDirection.UpStream, // from driver towards database server
                Toxicity = 1.0f,
            };

            bandWidthToxic.Attributes.Rate = 0;
            await proxy.AddAsync(bandWidthToxic);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error disabling connectivity UP-STREAM: {ex}");
        }

        Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} Disabled connectivity to {proxy.Name}");
    }
}
