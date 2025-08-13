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
    public static void EnableAllConnectivity()
    {
        foreach (Proxy proxy in TestEnvironment.Env.Proxies)
        {
            try
            {
                EnableConnectivity(proxy);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to disable proxy {proxy.Name}: {ex.Message}");
                throw;
            }
        }
    }

    public static void EnableConnectivity(string instanceName)
    {
        Proxy proxy = TestEnvironment.Env.GetProxy(instanceName);
        EnableConnectivity(proxy);
    }

    private static void EnableConnectivity(Proxy proxy)
    {
        try
        {
            foreach (ToxicBase toxic in proxy.GetAllToxics().Where(t => t.Name == "DOWN-STREAM" || t.Name == "UP-STREAM"))
            {
                try
                {
                    proxy.RemoveToxic(toxic.Name);
                }
                catch (Exception)
                {
                    // ignore
                    throw;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error enabling connectivity: {ex}");
            throw;
        }

        try
        {
            proxy.Enabled = true;
            proxy.Update();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error updating enable-connectivity proxy: {ex}");
            throw;
        }

        Console.WriteLine($"Enabled connectivity to {proxy.Name}");
    }

    public static void DisableAllConnectivity()
    {
        foreach (Proxy proxy in TestEnvironment.Env.Proxies)
        {
            DisableConnectivity(proxy);
        }
    }

    public static void DisableConnectivity(string instanceName)
    {
        Proxy proxy = TestEnvironment.Env.GetProxy(instanceName);
        DisableConnectivity(proxy);
    }

    private static void DisableConnectivity(Proxy proxy)
    {
        try
        {
            BandwidthToxic bandWidthToxic = new()
            {
                Name = "DOWN-STREAM",
                Stream = ToxicDirection.DownStream, // from database server towards driver
            };
            bandWidthToxic.Attributes.Rate = 0;
            proxy.Add(bandWidthToxic);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error disabling connectivity DOWN-STREAM: {ex}");
            throw;
        }

        try
        {
            BandwidthToxic bandWidthToxic = new()
            {
                Name = "UP-STREAM",
                Stream = ToxicDirection.UpStream, // from driver towards database server
            };
            bandWidthToxic.Attributes.Rate = 0;
            proxy.Add(bandWidthToxic);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error disabling connectivity UP-STREAM: {ex}");
            throw;
        }

        try
        {
            proxy.Enabled = true;
            proxy.Update();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error updating disable-connectivity proxy: {ex}");
            throw;
        }

        Console.WriteLine($"Disabled connectivity to {proxy.Name}");
    }
}
