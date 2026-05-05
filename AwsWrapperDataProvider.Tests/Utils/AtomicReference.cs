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

namespace AwsWrapperDataProvider.Tests.Utils;

public class AtomicReference<T>
    where T : class?
{
    private volatile T? value;

    public AtomicReference(T? initialValue = null)
    {
        this.value = initialValue;
    }

    public T? Get() => this.value;

    public void Set(T? newValue) => this.value = newValue;

    public bool CompareAndSet(T? expectedValue, T? newValue)
    {
        return ReferenceEquals(
            Interlocked.CompareExchange(ref this.value, newValue, expectedValue),
            expectedValue);
    }

    public T? GetAndSet(T? newValue)
    {
        return Interlocked.Exchange(ref this.value, newValue);
    }

    public T? GetAndUpdate(Func<T?, T?> updateFunction)
    {
        T? prev, next;
        do
        {
            prev = this.value;
            next = updateFunction(prev);
        }
        while (!this.CompareAndSet(prev, next));

        return prev;
    }

    public T? UpdateAndGet(Func<T?, T?> updateFunction)
    {
        T? prev, next;
        do
        {
            prev = this.value;
            next = updateFunction(prev);
        }
        while (!this.CompareAndSet(prev, next));

        return next;
    }

    public T? GetAndAccumulate(T? x, Func<T?, T?, T?> accumulatorFunction)
    {
        T? prev, next;
        do
        {
            prev = this.value;
            next = accumulatorFunction(prev, x);
        }
        while (!this.CompareAndSet(prev, next));

        return prev;
    }

    public T? AccumulateAndGet(T? x, Func<T?, T?, T?> accumulatorFunction)
    {
        T? prev, next;
        do
        {
            prev = this.value;
            next = accumulatorFunction(prev, x);
        }
        while (!this.CompareAndSet(prev, next));

        return next;
    }

    public static implicit operator T?(AtomicReference<T> atomicRef) => atomicRef.Get();

    public override string ToString() => this.value?.ToString() ?? "null";
}
