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

namespace AwsWrapperDataProvider.Performance.Tests;
using NPOI.SS.UserModel;

public class AdvancedPerformanceStatistics : IPerformanceStatistics
{
    public string ParameterDriverName { get; set; }

    public int ParameterFailoverDelayMs { get; set; }

    public double MinFailureDetectionTimeMs { get; set; }
    public double MaxFailureDetectionTimeMs { get; set; }
    public double AvgFailureDetectionTimeMs { get; set; }

    public double MinReconnectTimeMs { get; set; }
    public double MaxReconnectTimeMs { get; set; }
    public double AvgReconnectTimeMs { get; set; }

    public double MinDnsUpdateTimeMs { get; set; }
    public double MaxDnsUpdateTimeMs { get; set; }
    public double AvgDnsUpdateTimeMs { get; set; }

    public void WriteHeader(IRow row)
    {
        row.CreateCell(0).SetCellValue("Driver Configuration");
        row.CreateCell(1).SetCellValue("Failover Delay Ms");
        row.CreateCell(2).SetCellValue("Min Failure Detection Time Ms");
        row.CreateCell(3).SetCellValue("Max Failure Detection Time Ms");
        row.CreateCell(4).SetCellValue("Avg Failure Detection Time Ms");
        row.CreateCell(5).SetCellValue("Min Reconnect Time Ms");
        row.CreateCell(6).SetCellValue("Max Reconnect Time Ms");
        row.CreateCell(7).SetCellValue("Avg Reconnect Time Ms");
        row.CreateCell(8).SetCellValue("Min DNS Update Time Ms");
        row.CreateCell(9).SetCellValue("Max DNS Update Time Ms");
        row.CreateCell(10).SetCellValue("Avg DNS Update Time Ms");
    }

    public void WriteData(IRow row)
    {
        row.CreateCell(0).SetCellValue(this.ParameterDriverName);
        row.CreateCell(1).SetCellValue(this.ParameterFailoverDelayMs);
        row.CreateCell(2).SetCellValue(this.MinFailureDetectionTimeMs);
        row.CreateCell(3).SetCellValue(this.MaxFailureDetectionTimeMs);
        row.CreateCell(4).SetCellValue(this.AvgFailureDetectionTimeMs);
        row.CreateCell(5).SetCellValue(this.MinReconnectTimeMs);
        row.CreateCell(6).SetCellValue(this.MaxReconnectTimeMs);
        row.CreateCell(7).SetCellValue(this.AvgReconnectTimeMs);
        row.CreateCell(8).SetCellValue(this.MinDnsUpdateTimeMs);
        row.CreateCell(9).SetCellValue(this.MaxDnsUpdateTimeMs);
        row.CreateCell(10).SetCellValue(this.AvgDnsUpdateTimeMs);
    }
}
