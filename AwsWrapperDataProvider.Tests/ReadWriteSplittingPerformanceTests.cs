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

using System.Data;
using System.Diagnostics;
using AwsWrapperDataProvider.Driver.Plugins.ConnectTime;
using AwsWrapperDataProvider.Driver.Plugins.ExecutionTime;
using AwsWrapperDataProvider.Tests.Container.Utils;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;

namespace AwsWrapperDataProvider.Tests;
public class ReadWriteSplittingPerformanceTests : IntegrationTestBase
{
    private static readonly int RepeatTimes =
        int.TryParse(Environment.GetEnvironmentVariable("REPEAT_TIMES"), out var value)
                ? value
                : 10;
    private static readonly string PerfResultPath = Environment.GetEnvironmentVariable("PERF_RESULTS_DIR") ?? ".";
    private static readonly int TimeoutSec = 5;
    private static readonly int ConnectTimeoutSec = 5;
    private static readonly List<PerfStatSwitchConnection> SetReadOnlyPerfDataList = [];

    private readonly ITestOutputHelper logger;

    public ReadWriteSplittingPerformanceTests(ITestOutputHelper output)
    {
        this.logger = output;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql-rw-split-perf")]
    [Trait("Database", "pg-rw-split-perf")]
    [Trait("Engine", "aurora")]
    public async Task ConnectToWriter_SwitchReaderWriter(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 5, "Skipped due to test requiring number of database instances >= 5.");

        SetReadOnlyPerfDataList.Clear();

        var resultsWithoutPlugin = await this.GetSetReadOnlyResults("connectTime,executionTime", async, true);

        var resultsWithPluginWithConnectionPool = await this.GetSetReadOnlyResults("readWriteSplitting,connectTime,executionTime", async, true);

        var resultsWithPluginWithoutConnectionPool = await this.GetSetReadOnlyResults("readWriteSplitting,connectTime,executionTime", async, false);

        long switchToReaderMinOverhead = resultsWithPluginWithConnectionPool.SwitchToReaderMin - resultsWithoutPlugin.SwitchToReaderMin;
        long switchToReaderMaxOverhead = resultsWithPluginWithConnectionPool.SwitchToReaderMax - resultsWithoutPlugin.SwitchToReaderMax;
        long switchToReaderAvgOverhead = resultsWithPluginWithConnectionPool.SwitchToReaderAvg - resultsWithoutPlugin.SwitchToReaderAvg;

        long switchToWriterMinOverhead = resultsWithPluginWithConnectionPool.SwitchToWriterMin - resultsWithoutPlugin.SwitchToWriterMin;
        long switchToWriterMaxOverhead = resultsWithPluginWithConnectionPool.SwitchToWriterMax - resultsWithoutPlugin.SwitchToWriterMax;
        long switchToWriterAvgOverhead = resultsWithPluginWithConnectionPool.SwitchToWriterAvg - resultsWithoutPlugin.SwitchToWriterAvg;

        var connectReaderData = new PerfStatSwitchConnection
        {
            ConnectionSwitch = "Switch to reader",
            MinOverheadTime = switchToReaderMinOverhead,
            MaxOverheadTime = switchToReaderMaxOverhead,
            AvgOverheadTime = switchToReaderAvgOverhead,
        };
        SetReadOnlyPerfDataList.Add(connectReaderData);

        var connectWriterData = new PerfStatSwitchConnection
        {
            ConnectionSwitch = "Switch back to writer",
            MinOverheadTime = switchToWriterMinOverhead,
            MaxOverheadTime = switchToWriterMaxOverhead,
            AvgOverheadTime = switchToWriterAvgOverhead,
        };
        SetReadOnlyPerfDataList.Add(connectWriterData);

        var sync = async ? "Async" : "Sync";
        string fileWithConnectionPool = $@"{PerfResultPath}/{Engine}_{sync}_WithConnectionPool_ReadWriteSplittingPerformanceResults_{DateTime.Now:yyyyMMdd-HHmmss}.xlsx";
        this.WritePerfDataToFile(fileWithConnectionPool);

        SetReadOnlyPerfDataList.Clear();

        // Without connecition pool
        long connPoolSwitchToReaderMinOverhead = resultsWithPluginWithoutConnectionPool.SwitchToReaderMin - resultsWithoutPlugin.SwitchToReaderMin;
        long connPoolSwitchToReaderMaxOverhead = resultsWithPluginWithoutConnectionPool.SwitchToReaderMax - resultsWithoutPlugin.SwitchToReaderMax;
        long connPoolSwitchToReaderAvgOverhead = resultsWithPluginWithoutConnectionPool.SwitchToReaderAvg - resultsWithoutPlugin.SwitchToReaderAvg;

        long connPoolSwitchToWriterMinOverhead = resultsWithPluginWithoutConnectionPool.SwitchToWriterMin - resultsWithoutPlugin.SwitchToWriterMin;
        long connPoolSwitchToWriterMaxOverhead = resultsWithPluginWithoutConnectionPool.SwitchToWriterMax - resultsWithoutPlugin.SwitchToWriterMax;
        long connPoolSwitchToWriterAvgOverhead = resultsWithPluginWithoutConnectionPool.SwitchToWriterAvg - resultsWithoutPlugin.SwitchToWriterAvg;

        var noConnPoolsConnectReaderData = new PerfStatSwitchConnection
        {
            ConnectionSwitch = "Switch to reader",
            MinOverheadTime = connPoolSwitchToReaderMinOverhead,
            MaxOverheadTime = connPoolSwitchToReaderMaxOverhead,
            AvgOverheadTime = connPoolSwitchToReaderAvgOverhead,
        };
        SetReadOnlyPerfDataList.Add(noConnPoolsConnectReaderData);

        var noConnPoolsConnectWriterData = new PerfStatSwitchConnection
        {
            ConnectionSwitch = "Switch back to writer (use cached connection)",
            MinOverheadTime = connPoolSwitchToWriterMinOverhead,
            MaxOverheadTime = connPoolSwitchToWriterMaxOverhead,
            AvgOverheadTime = connPoolSwitchToWriterAvgOverhead,
        };
        SetReadOnlyPerfDataList.Add(noConnPoolsConnectWriterData);

        string fileWithoutConnectionPool = $@"{PerfResultPath}/{Engine}_{sync}_WithoutConnectionPool_ReadWriteSplittingPerformanceResults_{DateTime.Now:yyyyMMdd-HHmmss}.xlsx";
        this.WritePerfDataToFile(fileWithoutConnectionPool);
    }

    private void WritePerfDataToFile(string fileName)
    {
        if (SetReadOnlyPerfDataList == null || SetReadOnlyPerfDataList.Count == 0)
        {
            return;
        }

        this.logger.WriteLine("File name: {0}", fileName);

        using IWorkbook workbook = new XSSFWorkbook();
        ISheet sheet = workbook.CreateSheet("PerformanceResults");

        // Rows: header at 0, then data starts at 1
        for (int i = 0; i < SetReadOnlyPerfDataList.Count; i++)
        {
            var perfStat = SetReadOnlyPerfDataList[i];

            if (i == 0)
            {
                // header row
                var headerRow = sheet.CreateRow(0);
                perfStat.WriteHeader(headerRow);
            }

            var dataRow = sheet.CreateRow(i + 1);
            perfStat.WriteData(dataRow);
        }

        string? dir = Path.GetDirectoryName(fileName);
        if (!string.IsNullOrEmpty(dir))
        {
            Directory.CreateDirectory(dir);
        }

        using var fs = new FileStream(fileName, FileMode.Create);
        workbook.Write(fs);
        var fullPath = Path.GetFullPath(fileName);
        this.logger.WriteLine("Full path: {0}", fullPath);
    }

    private async Task<Result> GetSetReadOnlyResults(string plugins, bool async, bool connectionPool = true)
    {
        var elapsedSwitchToReaderTimes = new List<double>(RepeatTimes);
        var elapsedSwitchToWriterTimes = new List<double>(RepeatTimes);
        var result = new Result();

        for (int i = 0; i < RepeatTimes; i++)
        {
            var connectionString = ConnectionStringHelper.GetUrl(
                Engine,
                Endpoint,
                Port,
                Username,
                Password,
                DefaultDbName,
                TimeoutSec,
                ConnectTimeoutSec,
                plugins,
                connectionPool);

            using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
            await AuroraUtils.OpenDbConnection(connection, async);
            Assert.Equal(ConnectionState.Open, connection.State);

            // Measure switch to reader
            ConnectTimePlugin.ReseConnectTime();
            ExecutionTimePlugin.ResetExecutionTime();
            var sw = Stopwatch.StartNew();
            await AuroraUtils.SetReadOnly(connection, Engine, true, async);
            var connectTimeNs = ConnectTimePlugin.GetTotalConnectTime();
            var executionTimeNs = ExecutionTimePlugin.GetTotalExecutionTime();

            sw.Stop();
            long ticks = sw.ElapsedTicks;
            double elapsedReaderNs = (double)ticks * 1_000_000_000.0 / Stopwatch.Frequency;
            elapsedSwitchToReaderTimes.Add(elapsedReaderNs - connectTimeNs - executionTimeNs);

            // Measure switch to writer
            ConnectTimePlugin.ReseConnectTime();
            ExecutionTimePlugin.ResetExecutionTime();

            sw = Stopwatch.StartNew();
            await AuroraUtils.SetReadOnly(connection, Engine, false, async);
            connectTimeNs = ConnectTimePlugin.GetTotalConnectTime();
            executionTimeNs = ExecutionTimePlugin.GetTotalExecutionTime();
            sw.Stop();
            ticks = sw.ElapsedTicks;
            double elapsedWriterNs = (double)ticks * 1_000_000_000.0 / Stopwatch.Frequency;
            elapsedSwitchToWriterTimes.Add(elapsedWriterNs);
        }

        // Summary stats for reader
        result.SwitchToReaderMin = (long)elapsedSwitchToReaderTimes.Min();
        result.SwitchToReaderMax = (long)elapsedSwitchToReaderTimes.Max();
        result.SwitchToReaderAvg = (long)elapsedSwitchToReaderTimes.Average();

        // Summary stats for writer
        result.SwitchToWriterMin = (long)elapsedSwitchToWriterTimes.Min();
        result.SwitchToWriterMax = (long)elapsedSwitchToWriterTimes.Max();
        result.SwitchToWriterAvg = (long)elapsedSwitchToWriterTimes.Average();

        return result;
    }

    private class Result
    {
        public long SwitchToReaderMin { get; set; }
        public long SwitchToReaderMax { get; set; }
        public long SwitchToReaderAvg { get; set; }
        public long SwitchToWriterMin { get; set; }
        public long SwitchToWriterMax { get; set; }
        public long SwitchToWriterAvg { get; set; }
    }

    private abstract class PerfStatBase
    {
        public abstract void WriteHeader(IRow row);
        public abstract void WriteData(IRow row);
    }

    private class PerfStatSwitchConnection : PerfStatBase
    {
        public string? ConnectionSwitch;
        public long MinOverheadTime;
        public long MaxOverheadTime;
        public long AvgOverheadTime;

        public override void WriteHeader(IRow row)
        {
            ICell cell = row.CreateCell(0);
            cell.SetCellValue(string.Empty);

            cell = row.CreateCell(1);
            cell.SetCellValue("minOverheadTimeNanos");

            cell = row.CreateCell(2);
            cell.SetCellValue("maxOverheadTimeNanos");

            cell = row.CreateCell(3);
            cell.SetCellValue("avgOverheadTimeNanos");
        }

        public override void WriteData(IRow row)
        {
            ICell cell = row.CreateCell(0);
            cell.SetCellValue(this.ConnectionSwitch);

            // NPOI's SetCellValue for numerics uses double; cast long -> double.
            cell = row.CreateCell(1);
            cell.SetCellValue((double)this.MinOverheadTime);

            cell = row.CreateCell(2);
            cell.SetCellValue((double)this.MaxOverheadTime);

            cell = row.CreateCell(3);
            cell.SetCellValue((double)this.AvgOverheadTime);
        }
    }
}
