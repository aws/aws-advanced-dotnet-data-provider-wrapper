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

namespace AwsWrapperDataProvider.Driver.Plugins.BlueGreenConnection;

public static class BlueGreenPhase
{
    private static readonly Dictionary<string, BlueGreenPhaseType> BlueGreenStatusMapping = new()
    {
        { "AVAILABLE", BlueGreenPhaseType.CREATED },
        { "SWITCHOVER_INITIATED", BlueGreenPhaseType.PREPARATION },
        { "SWITCHOVER_IN_PROGRESS", BlueGreenPhaseType.IN_PROGRESS },
        { "SWITCHOVER_IN_POST_PROCESSING", BlueGreenPhaseType.POST },
        { "SWITCHOVER_COMPLETED", BlueGreenPhaseType.COMPLETED },
    };

    private static readonly Dictionary<BlueGreenPhaseType, bool> ActiveSwitchoverOrCompletedMapping = new()
    {
        { BlueGreenPhaseType.NOT_CREATED, false },
        { BlueGreenPhaseType.CREATED, false },
        { BlueGreenPhaseType.PREPARATION, true },
        { BlueGreenPhaseType.IN_PROGRESS, true },
        { BlueGreenPhaseType.POST, true },
        { BlueGreenPhaseType.COMPLETED, true },
    };

    public static BlueGreenPhaseType ParsePhase(string value, string version)
    {
        if (string.IsNullOrEmpty(value))
        {
            return BlueGreenPhaseType.NOT_CREATED;
        }

        if (!BlueGreenStatusMapping.TryGetValue(value.ToUpperInvariant(), out var phase))
        {
            throw new ArgumentException(string.Format(Resources.BlueGreenRole_ParseRole_ExceptionValue, value));
        }

        return phase;
    }

    public static bool IsActiveSwitchoverOrCompleted(this BlueGreenPhaseType phaseType)
    {
        return ActiveSwitchoverOrCompletedMapping[phaseType];
    }
}
