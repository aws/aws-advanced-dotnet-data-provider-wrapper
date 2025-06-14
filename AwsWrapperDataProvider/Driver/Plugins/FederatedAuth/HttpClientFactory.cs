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

using System.Net;
using System.Net.Http;
using ZstdSharp.Unsafe;

namespace AwsWrapperDataProvider.Driver.Plugins.FederatedAuth;

public class HttpClientFactory
{
    private class LaxRedirectHandler()
        : DelegatingHandler(new HttpClientHandler { AllowAutoRedirect = false })
    {
        // arbitrary max redirects to prevent an infinite loop due to bad server
        private readonly int maxRedirects = 10;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            int numRedirects = 0;
            HttpResponseMessage response = await base.SendAsync(request, cancellationToken);

            while (ResponseIsRedirect(response) && numRedirects++ < this.maxRedirects)
            {
                Uri redirectUri = response.Headers.Location ?? throw new Exception("Redirect location not provided");

                if (!redirectUri.IsAbsoluteUri)
                {
                    Uri requestUri = request.RequestUri ?? throw new Exception("Request URI not provided");
                    redirectUri = new Uri(requestUri, redirectUri);
                }

                HttpMethod method = request.Method;

                // some redirects imply a change of method from POST to GET
                if ((response.StatusCode == HttpStatusCode.MovedPermanently ||
                    response.StatusCode == HttpStatusCode.Found ||
                    response.StatusCode == HttpStatusCode.SeeOther) &&
                    request.Method == HttpMethod.Post)
                {
                    method = HttpMethod.Get;
                }

                HttpRequestMessage redirectedRequest = new(method, redirectUri);

                // if method has changed, don't reuse body of original request
                redirectedRequest.Content = method == request.Method ? request.Content : null;

                foreach (KeyValuePair<string, IEnumerable<string>> header in request.Headers)
                {
                    redirectedRequest.Headers.TryAddWithoutValidation(header.Key, header.Value);
                }

                request = redirectedRequest;
                response.Dispose();
                response = await base.SendAsync(request, cancellationToken);
            }

            return response;
        }

        private static bool ResponseIsRedirect(HttpResponseMessage response)
        {
            return (int)response.StatusCode >= 300 && (int)response.StatusCode < 400 && response.Headers.Location != null;
        }
    }

    public static HttpClient GetDisposableHttpClient(int connectionTimeoutMs)
    {
        HttpClient client = new(new LaxRedirectHandler());
        client.Timeout = TimeSpan.FromMilliseconds(connectionTimeoutMs);

        return client;
    }
}
