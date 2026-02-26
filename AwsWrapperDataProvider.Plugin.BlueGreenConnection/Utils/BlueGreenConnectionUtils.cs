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

using System.Text.RegularExpressions;
using AwsWrapperDataProvider.Driver.Dialects;

namespace AwsWrapperDataProvider.Plugin.BlueGreenConnection.Utils;

public class BlueGreenConnectionUtils
{
    private static readonly Regex BgGreenHostPattern = 
        new(@".*(?<prefix>-green-[0-9a-z]{6})\..*", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex BgGreenHostIdPattern = 
        new(@"(.*)-green-[0-9a-z]{6}", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex BgOldHostPattern = 
        new(@".*(?<prefix>-old1)\..*", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static Func<string, string>? prepareHostFunc;

    public static void SetPrepareHostFunc(Func<string, string>? func)
    {
        prepareHostFunc = func;
    }

    public static void ResetPrepareHostFunc()
    {
        prepareHostFunc = null;
    }

    private static string GetPreparedHost(string host)
    {
        var func = prepareHostFunc;
        if (func == null)
        {
            return host;
        }
        var preparedHost = func(host);
        return preparedHost ?? host;
    }

    public static string RemoveGreenInstancePrefix(string? host)
    {
        if (string.IsNullOrEmpty(host))
        {
            return host ?? string.Empty;
        }

        var preparedHost = GetPreparedHost(host);
        var match = BgGreenHostPattern.Match(preparedHost);

        if (!match.Success)
        {
            var hostIdMatch = BgGreenHostIdPattern.Match(preparedHost);
            if (!hostIdMatch.Success)
            {
                return host;
            }
            return hostIdMatch.Groups[1].Value;
        }

        var prefix = match.Groups["prefix"].Value;
        if (string.IsNullOrEmpty(prefix))
        {
            return host;
        }

        return host.Replace(prefix + ".", ".");
    }

    public bool IsNotOldInstance(string host)
    {
        var preparedHost = GetPreparedHost(host);
        return string.IsNullOrEmpty(preparedHost) || !BgOldHostPattern.IsMatch(preparedHost);
    }
}

