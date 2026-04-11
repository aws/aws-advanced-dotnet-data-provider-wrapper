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

using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Tests.Driver.Utils;

public class RWQueueTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void Enqueue_SingleItem_DequeueReturnsThatItemWithTrue()
    {
        using var queue = new RWQueue<int>();
        queue.Enqueue(42);

        var (item, success) = queue.Dequeue();

        Assert.True(success);
        Assert.Equal(42, item);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Enqueue_MultipleItems_DequeueReturnsInFIFOOrder()
    {
        using var queue = new RWQueue<string>();
        queue.Enqueue("A");
        queue.Enqueue("B");
        queue.Enqueue("C");

        var (item1, success1) = queue.Dequeue();
        var (item2, success2) = queue.Dequeue();
        var (item3, success3) = queue.Dequeue();

        Assert.True(success1);
        Assert.Equal("A", item1);
        Assert.True(success2);
        Assert.Equal("B", item2);
        Assert.True(success3);
        Assert.Equal("C", item3);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Dequeue_EmptyQueue_ReturnsDefaultAndFalse()
    {
        using var queue = new RWQueue<int>();

        var (item, success) = queue.Dequeue();

        Assert.False(success);
        Assert.Equal(default, item);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Count_ReflectsNumberOfEnqueuedItems()
    {
        using var queue = new RWQueue<int>();
        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);

        Assert.Equal(3, queue.Count);

        queue.Dequeue();

        Assert.Equal(2, queue.Count);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsEmpty_NewQueue_ReturnsTrue()
    {
        using var queue = new RWQueue<int>();

        Assert.True(queue.IsEmpty);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IsEmpty_AfterEnqueue_ReturnsFalse()
    {
        using var queue = new RWQueue<int>();
        queue.Enqueue(1);

        Assert.False(queue.IsEmpty);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ForEach_VisitsAllItemsInFIFOOrder()
    {
        using var queue = new RWQueue<string>();
        queue.Enqueue("A");
        queue.Enqueue("B");
        queue.Enqueue("C");

        var collected = new List<string>();
        queue.ForEach(item => collected.Add(item));

        Assert.Equal(["A", "B", "C"], collected);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RemoveIf_RemovesMatchingKeepsNonMatchingInOrder()
    {
        using var queue = new RWQueue<int>();
        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        queue.Enqueue(4);
        queue.Enqueue(5);

        queue.RemoveIf(x => x % 2 == 0);

        var remaining = new List<int>();
        queue.ForEach(item => remaining.Add(item));
        Assert.Equal([1, 3, 5], remaining);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RemoveIf_NoMatchPredicate_LeavesQueueUnchanged()
    {
        using var queue = new RWQueue<int>();
        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);

        queue.RemoveIf(x => x > 10);

        Assert.Equal(3, queue.Count);
        var remaining = new List<int>();
        queue.ForEach(item => remaining.Add(item));
        Assert.Equal([1, 2, 3], remaining);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RemoveIf_AllMatchPredicate_EmptiesQueue()
    {
        using var queue = new RWQueue<int>();
        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);

        queue.RemoveIf(x => true);

        Assert.True(queue.IsEmpty);
        Assert.Equal(0, queue.Count);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Enqueue_InsideForEach_ThrowsInvalidOperationException()
    {
        using var queue = new RWQueue<int>();
        queue.Enqueue(1);

        Assert.Throws<InvalidOperationException>(() =>
            queue.ForEach(_ => queue.Enqueue(99)));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Dequeue_InsideForEach_ThrowsInvalidOperationException()
    {
        using var queue = new RWQueue<int>();
        queue.Enqueue(1);

        Assert.Throws<InvalidOperationException>(() =>
            queue.ForEach(_ => queue.Dequeue()));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RemoveIf_InsideForEach_ThrowsInvalidOperationException()
    {
        using var queue = new RWQueue<int>();
        queue.Enqueue(1);

        Assert.Throws<InvalidOperationException>(() =>
            queue.ForEach(_ => queue.RemoveIf(x => true)));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Dispose_ThenEnqueue_ThrowsObjectDisposedException()
    {
        var queue = new RWQueue<int>();
        queue.Dispose();

        Assert.Throws<ObjectDisposedException>(() => queue.Enqueue(1));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Dispose_CalledTwice_DoesNotThrow()
    {
        var queue = new RWQueue<int>();

        var exception = Record.Exception(() =>
        {
            queue.Dispose();
            queue.Dispose();
        });

        Assert.Null(exception);
    }
}
