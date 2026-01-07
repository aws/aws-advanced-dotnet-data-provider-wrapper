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

using System.Collections.Concurrent;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;

namespace AwsWrapperDataProvider.Performance.Tests;

public static class SheetsWriter
{
    public static void WriteAdvancedPerformanceDataToFile(string fileName, ConcurrentQueue<AdvancedPerformanceStatistics> dataList)
    {
        if (dataList.IsEmpty)
        {
            return;
        }

        var sortedData = dataList
            .OrderBy(d => d.ParameterFailoverDelayMs)
            .ThenBy(d => d.ParameterDriverName)
            .ToList();

        WriteDataToFile(fileName, sortedData);
    }

    public static void WritePerformanceDataToFile(string fileName, ConcurrentQueue<PerformanceStatistics> dataList)
    {
        if (dataList.IsEmpty)
        {
            return;
        }

        WriteDataToFile(fileName, dataList.ToList());
    }

    private static void WriteDataToFile<T>(string fileName, List<T> dataList)
        where T : IPerformanceStatistics
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("PerformanceResults");

        dataList[0].WriteHeader(sheet.CreateRow(0));

        for (int rows = 0; rows < dataList.Count; rows++)
        {
            dataList[rows].WriteData(sheet.CreateRow(rows + 1));
        }

        using var fileOut = new FileStream(fileName, FileMode.Create);
        workbook.Write(fileOut);
    }
}
