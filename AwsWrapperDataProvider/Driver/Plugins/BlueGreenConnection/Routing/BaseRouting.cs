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

using System.Diagnostics;

namespace AwsWrapperDataProvider.Driver.Plugins.BlueGreenConnection.Routing;

public abstract class BaseRouting
{
    private const long SleepPeriod = 50L;

    protected async Task Delay(long delayMs, BlueGreenStatus? bgStatus, string bgdId, CancellationToken cancellationToken)
    {
        if (bgStatus == null)
        {
            await Task.Delay((int)delayMs, cancellationToken);
            return;
        }

        var sw = Stopwatch.StartNew();
        var minDelay = (int)Math.Min(delayMs, SleepPeriod);

        do
        {
            // Use Monitor.Wait so UpdateStatusCache can wake us up with PulseAll
            await Task.Run(
                () =>
            {
                lock (bgStatus)
                {
                    Monitor.Wait(bgStatus, minDelay);
                }
            },
                cancellationToken);
        }
        while (ReferenceEquals(bgStatus, BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(bgdId))
               && sw.ElapsedMilliseconds < delayMs
               && !cancellationToken.IsCancellationRequested);
    }
}
