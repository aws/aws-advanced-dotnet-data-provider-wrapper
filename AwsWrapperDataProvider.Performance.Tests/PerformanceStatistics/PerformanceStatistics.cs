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

using NPOI.SS.UserModel;

namespace AwsWrapperDataProvider.Performance.Tests.PerformanceStatistics;

public class PerformanceStatistics : IPerformanceStatistics
{
    public int ParameterDetectionTime { get; set; }

    public int ParameterDetectionInterval { get; set; }

    public long ParameterDetectionCount { get; set; }

    public int ParameterNetworkOutageDelayMs { get; set; }

    public long MinFailureDetectionTimeMs { get; set; }

    public long MaxFailureDetectionTimeMs { get; set; }

    public long AvgFailureDetectionTimeMs { get; set; }

    public void WriteHeader(IRow row)
    {
        row.CreateCell(0).SetCellValue("Failure Detection Grace Time");
        row.CreateCell(1).SetCellValue("Failure Detection Interval");
        row.CreateCell(2).SetCellValue("Failure Detection Count");
        row.CreateCell(3).SetCellValue("Network Outage Delay Ms");
        row.CreateCell(4).SetCellValue("Min Failure Detection Time Ms");
        row.CreateCell(5).SetCellValue("Max Failure Detection Time Ms");
        row.CreateCell(6).SetCellValue("Avg Failure Detection Time Ms");
    }

    public void WriteData(IRow row)
    {
        row.CreateCell(0).SetCellValue(this.ParameterDetectionTime);
        row.CreateCell(1).SetCellValue(this.ParameterDetectionInterval);
        row.CreateCell(2).SetCellValue(this.ParameterDetectionCount);
        row.CreateCell(3).SetCellValue(this.ParameterNetworkOutageDelayMs);
        row.CreateCell(4).SetCellValue(this.MinFailureDetectionTimeMs);
        row.CreateCell(5).SetCellValue(this.MaxFailureDetectionTimeMs);
        row.CreateCell(6).SetCellValue(this.AvgFailureDetectionTimeMs);
    }

    public override string ToString()
    {
        return
            $"{base.ToString()} " +
            $"[\nParameterDetectionTime={this.ParameterDetectionTime}," +
            $"\nParameterDetectionInterval={this.ParameterDetectionInterval}," +
            $"\nParameterDetectionCount={this.ParameterDetectionCount}," +
            $"\nParamNetworkOutageDelayMs={this.ParameterNetworkOutageDelayMs} " +
            $"\nMinFailureDetectionTimeMs={this.MinFailureDetectionTimeMs} " +
            $"\nMaxFailureDetectionTimeMs={this.MaxFailureDetectionTimeMs} " +
            $"\nAvgFailureDetectionTimeMs={this.AvgFailureDetectionTimeMs} ]";
    }
}
