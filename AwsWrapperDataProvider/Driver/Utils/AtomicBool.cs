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

public class AtomicBool
{
    private int value;

    public AtomicBool(bool initialValue = false)
    {
        this.value = initialValue ? 1 : 0;
    }

    // Read
    public bool Get() => Volatile.Read(ref this.value) == 1;

    // Write
    public void Set(bool newValue) => Volatile.Write(ref this.value, newValue ? 1 : 0);

    // Compare and set (returns true if successful)
    public bool CompareAndSet(bool expected, bool newValue)
    {
        int expectedInt = expected ? 1 : 0;
        int newInt = newValue ? 1 : 0;
        return Interlocked.CompareExchange(ref this.value, newInt, expectedInt) == expectedInt;
    }

    // Get and set (returns old value)
    public bool GetAndSet(bool newValue)
    {
        int newInt = newValue ? 1 : 0;
        return Interlocked.Exchange(ref this.value, newInt) == 1;
    }

    // Implicit conversion to bool
    public static implicit operator bool(AtomicBool atomicBool) => atomicBool.Get();

    public override string ToString() => this.Get().ToString();
}
