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

using AwsWrapperDataProvider.Properties;

namespace AwsWrapperDataProvider.Driver.Utils;

public class RWQueue<T> : IDisposable
{
    private readonly List<T> _items = new();
    private readonly ReaderWriterLockSlim _lock = new();
    private readonly ThreadLocal<bool> _isIterating = new(() => false);
    private bool _disposed;

    private void ThrowIfReentrant()
    {
        if (this._isIterating.Value)
        {
            throw new InvalidOperationException(
                Resources.RWQueue_ThrowIfReentrant_CannotModifyDuringForEach);
        }
    }

    public void Enqueue(T item)
    {
        this.ThrowIfReentrant();
        this._lock.EnterWriteLock();
        try
        {
            this._items.Add(item);
        }
        finally
        {
            this._lock.ExitWriteLock();
        }
    }

    public (T?, bool) Dequeue()
    {
        this.ThrowIfReentrant();
        this._lock.EnterWriteLock();
        try
        {
            if (this._items.Count == 0)
            {
                return (default, false);
            }

            var item = this._items[0];
            this._items.RemoveAt(0);
            return (item, true);
        }
        finally
        {
            this._lock.ExitWriteLock();
        }
    }

    public void RemoveIf(Func<T, bool> predicate)
    {
        this.ThrowIfReentrant();
        this._lock.EnterWriteLock();
        try
        {
            this._items.RemoveAll(new Predicate<T>(predicate));
        }
        finally
        {
            this._lock.ExitWriteLock();
        }
    }

    public bool IsEmpty
    {
        get
        {
            this._lock.EnterReadLock();
            try
            {
                return this._items.Count == 0;
            }
            finally
            {
                this._lock.ExitReadLock();
            }
        }
    }

    public int Count
    {
        get
        {
            this._lock.EnterReadLock();
            try
            {
                return this._items.Count;
            }
            finally
            {
                this._lock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Iterates over all items in the queue in FIFO order under a read lock.
    /// This method is intended for read-only inspection of items.
    /// The provided delegate must not call write operations (Enqueue, Dequeue, RemoveIf)
    /// on this same RWQueue instance — doing so will throw <see cref="InvalidOperationException"/>.
    /// </summary>
    public void ForEach(Action<T> action)
    {
        this._lock.EnterReadLock();
        try
        {
            try
            {
                this._isIterating.Value = true;
                foreach (var item in this._items)
                {
                    action(item);
                }
            }
            finally
            {
                this._isIterating.Value = false;
            }
        }
        finally
        {
            this._lock.ExitReadLock();
        }
    }

    public void Dispose()
    {
        if (this._disposed)
        {
            return;
        }

        this._disposed = true;
        this._lock.Dispose();
        this._isIterating.Dispose();
    }
}
