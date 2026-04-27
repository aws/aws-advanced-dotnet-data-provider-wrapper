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

public class AtomicLong
{
    private long value;

    public AtomicLong(long initialValue = 0)
    {
        this.value = initialValue;
    }

    public long Get() => Interlocked.Read(ref this.value);

    public void Set(long newValue) => Interlocked.Exchange(ref this.value, newValue);

    public long GetAndSet(long newValue) => Interlocked.Exchange(ref this.value, newValue);

    public long IncrementAndGet() => Interlocked.Increment(ref this.value);

    public long DecrementAndGet() => Interlocked.Decrement(ref this.value);

    public long AddAndGet(long delta) => Interlocked.Add(ref this.value, delta);

    public bool CompareAndSet(long expected, long update) => Interlocked.CompareExchange(ref this.value, update, expected) == expected;

    public override string ToString() => this.Get().ToString();
}
