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

using AwsWrapperDataProvider.Plugin.BlueGreenConnection.BlueGreenConnection;

namespace AwsWrapperDataProvider.Plugin.BlueGreenConnection.Routing;

public abstract class BaseRouting
{
    protected const long SleepChunk = 50L;

    protected virtual long GetNanoTime()
    {
        return DateTime.UtcNow.Ticks * 100;
    }

    protected void Delay(long delayMs, BlueGreenStatus? bgStatus, string bgdId, CancellationToken cancellationToken)
    {
        long start = this.GetNanoTime();
        long end = start + (delayMs * 1_000_000);
        long minDelay = Math.Min(delayMs, SleepChunk);

        if (bgStatus == null)
        {
            Thread.Sleep((int)delayMs);
        }
        else
        {
            do
            {
                lock (bgStatus)
                {
                    Monitor.Wait(bgStatus, (int)minDelay);
                }
            }
            while (ReferenceEquals(bgStatus, BlueGreenConnectionCache.Instance.Get<BlueGreenStatus>(bgdId))
                     && this.GetNanoTime() < end
                     && !cancellationToken.IsCancellationRequested);
        }
    }
}
