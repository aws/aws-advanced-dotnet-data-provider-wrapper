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

using Amazon.XRay.Recorder.Core;
using Amazon.XRay.Recorder.Core.Exceptions;
using Amazon.XRay.Recorder.Core.Internal.Emitters;
using Amazon.XRay.Recorder.Core.Internal.Entities;
using Amazon.XRay.Recorder.Core.Sampling;
using Amazon.XRay.Recorder.Core.Strategies;

namespace AwsWrapperDataProvider.Telemetry.XRay.Tests;

/// <summary>
/// Test harness that swaps the <see cref="AWSXRayRecorder.Instance"/>
/// singleton's emitter, sampling strategy, and context-missing strategy so
/// unit tests can observe emitted segments without hitting a daemon and
/// without leaking state between tests.
/// </summary>
/// <remarks>
/// <para>
/// Instantiate inside a test constructor and dispose at the end of the test.
/// All X-Ray tests should share the collection defined by
/// <see cref="XRayTestCollection"/> so parallel test execution does not race
/// on the singleton state.
/// </para>
/// </remarks>
internal sealed class XRayTestHarness : IDisposable
{
    private readonly ISegmentEmitter originalEmitter;
    private readonly ContextMissingStrategy originalContextMissing;
    private readonly ISamplingStrategy originalSampling;

    public XRayTestHarness()
    {
        AWSXRayRecorder recorder = AWSXRayRecorder.Instance;
        this.originalEmitter = recorder.Emitter;
        this.originalContextMissing = recorder.ContextMissingStrategy;
        this.originalSampling = recorder.SamplingStrategy;

        // Swap in a capturing emitter so Send() calls do not hit UDP/the
        // local daemon, and so tests can assert on emitted segments.
        this.Emitter = new CapturingEmitter();
        recorder.Emitter = this.Emitter;

        // LOG_ERROR so that mis-ordered context operations do not throw and
        // derail tests.
        recorder.ContextMissingStrategy = ContextMissingStrategy.LOG_ERROR;

        // Force every segment to be sampled so emission is deterministic.
        recorder.SamplingStrategy = new AlwaysSampleStrategy();

        // Start with a clean trace context so state from a previous test
        // does not leak into this test's isolation.
        TryClearEntity(recorder);
    }

    public CapturingEmitter Emitter { get; }

    public void Dispose()
    {
        AWSXRayRecorder recorder = AWSXRayRecorder.Instance;
        TryClearEntity(recorder);
        recorder.SamplingStrategy = this.originalSampling;
        recorder.ContextMissingStrategy = this.originalContextMissing;
        recorder.Emitter = this.originalEmitter;
    }

    private static void TryClearEntity(AWSXRayRecorder recorder)
    {
        try
        {
            recorder.ClearEntity();
        }
        catch (EntityNotAvailableException)
        {
            // Already clear.
        }
    }

    /// <summary>
    /// <see cref="ISegmentEmitter"/> that records every segment handed to it
    /// so tests can assert on post-emission state.
    /// </summary>
    internal sealed class CapturingEmitter : ISegmentEmitter
    {
        private readonly object @lock = new();
        private readonly List<Entity> sent = new();

        public IReadOnlyList<Entity> Sent
        {
            get
            {
                lock (this.@lock)
                {
                    return this.sent.ToArray();
                }
            }
        }

        public void Send(Entity entity)
        {
            lock (this.@lock)
            {
                this.sent.Add(entity);
            }
        }

        public void SetDaemonAddress(string daemonAddress)
        {
            // No-op for tests.
        }

        public void Dispose()
        {
            // No-op; state lives as long as the test.
        }
    }

    /// <summary>
    /// <see cref="ISamplingStrategy"/> that unconditionally samples every
    /// request so tests do not depend on ambient sampling configuration.
    /// </summary>
    private sealed class AlwaysSampleStrategy : ISamplingStrategy
    {
        public SamplingResponse ShouldTrace(SamplingInput input) => new(SampleDecision.Sampled);
    }
}

/// <summary>
/// xunit collection definition used to serialize tests that mutate the
/// <see cref="AWSXRayRecorder.Instance"/> singleton.
/// </summary>
[CollectionDefinition("X-Ray serial", DisableParallelization = true)]
public class XRayTestCollection
{
}
