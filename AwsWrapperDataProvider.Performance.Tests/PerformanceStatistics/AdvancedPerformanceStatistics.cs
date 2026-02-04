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

namespace AwsWrapperDataProvider.Performance.Tests.PerformanceStatistics;
using NPOI.SS.UserModel;

public class AdvancedPerformanceStatistics : IPerformanceStatistics
{
    public string? ParameterDriverName { get; set; }
    public double FailureDelayMs { get; set; }
    public double EfmDetectionTime { get; set; }
    public double FailoverEfmDetectionTime { get; set; }
    public double DirectDriverDetectionTime { get; set; }
    public double DnsUpdateTimeMs { get; set; }

    public void WriteHeader(IRow row)
    {
        row.CreateCell(0).SetCellValue("Driver Configuration");
        row.CreateCell(1).SetCellValue("Failover Delay Ms");
        row.CreateCell(2).SetCellValue("EFM Detection Time");
        row.CreateCell(3).SetCellValue("Failover/EFM Detection Time");
        row.CreateCell(4).SetCellValue("DirectDriver Detection Time");
        row.CreateCell(5).SetCellValue("DNS Update Time");
    }

    public void WriteData(IRow row)
    {
        row.CreateCell(0).SetCellValue(this.ParameterDriverName);
        row.CreateCell(1).SetCellValue(this.FailureDelayMs);
        row.CreateCell(2).SetCellValue(this.EfmDetectionTime);
        row.CreateCell(3).SetCellValue(this.FailoverEfmDetectionTime);
        row.CreateCell(4).SetCellValue(this.DirectDriverDetectionTime);
        row.CreateCell(5).SetCellValue(this.DnsUpdateTimeMs);
    }
}
