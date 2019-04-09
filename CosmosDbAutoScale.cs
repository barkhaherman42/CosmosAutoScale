using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using Newtonsoft.Json;
using System.Collections.Generic;


namespace CosmosDbScaleUp
{
    public static class CosmosDbScaleUp
    {
        private static string _subscriptionId;
        private static string _tenantId;
        private static string _applicationId;
        private static string _applicationPwd;

        private static string _resourceGroupName;
        private static string _accountName;
        private static string _collName;
        private static string _dbName;
        private static string _accountUri;
        private static string _accountKey;
        private static string _maxAllowedThroughput;
        private static string _minAllowedThroughput;
        private static string _throughputStep;

        private static DocumentClient docDbClient;

        private const string AzureManagementUri = "https://management.azure.com";
        private const string ApiVersion = "2014-04-01";

        [FunctionName("CosmosDbScaleUp")]
        public static void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, TraceWriter log)

        {
            _subscriptionId = GetEnvironmentVariable("subscriptionId");
            _tenantId = GetEnvironmentVariable("tenantId");
            _applicationId = GetEnvironmentVariable("applicationId");
            _applicationPwd = GetEnvironmentVariable("applicationPwd");
            _resourceGroupName = GetEnvironmentVariable("resourceGroupName");
            _accountName = GetEnvironmentVariable("accountName");
            _accountUri = GetEnvironmentVariable("accountUri");
            _accountKey = GetEnvironmentVariable("authKey");
            _dbName = GetEnvironmentVariable("dbName");
            _collName = GetEnvironmentVariable("collName");
            _maxAllowedThroughput = GetEnvironmentVariable("maxAllowedThroughput");
            _minAllowedThroughput = GetEnvironmentVariable("minAllowedThroughput");
            _throughputStep = GetEnvironmentVariable("throughputStep");
            //Console.WriteLine("accountUri: " + _accountUri);

            log.Info($"Timer trigger function executed at: {DateTime.Now}");

            docDbClient = new DocumentClient(
                new Uri(_accountUri),
                 _accountKey,
                new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp
                });

            DocumentCollection collection = GetCollection(_dbName, _collName);

            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(_dbName, collection.Id);
            FeedResponse<PartitionKeyRange> pkRanges = docDbClient.ReadPartitionKeyRangeFeedAsync(
                    collectionUri).Result;
            int partitionCount = pkRanges.Count;
            log.Info($"The number of partitions in the collection is {partitionCount}");

            int provisionedThroughput = GetOfferThroughput(collection.SelfLink);
            log.Info($"Provisioned throughtput is {provisionedThroughput}");
            int partitionThroughput = provisionedThroughput / partitionCount;


            MetricCollection metricList = ReadMetricValues(collection.SelfLink, GetAccessToken());
           
            int throughputIncrease = 0;

            bool isCollectionBusy = false;
            bool isCollectionOverProvisioned = true;
            foreach (var metric in metricList.Value)
            {
                foreach (var metricValue in metric.MetricValues)
                {
                    log.Info($"Max RUs Per Second is {metricValue.Maximum}. Throughtput per partition is {partitionThroughput}");

                    if (metricValue.Maximum > partitionThroughput)
                    {
                        isCollectionBusy = true;
                        isCollectionOverProvisioned = false;
                        log.Info($"Partition throughput increased more than provisioned at {metricValue.Maximum} - {metricValue.Timestamp}");
                        throughputIncrease = (int)metricValue.Maximum * partitionCount;
                        break;
                    }
                    else if(partitionThroughput > metricValue.Maximum)
                    {
                        isCollectionOverProvisioned = true;
                    }
                }
            }

            if (isCollectionBusy)
            {
                if (throughputIncrease > Int32.Parse(_maxAllowedThroughput))
                {
                    log.Info($"Max throughput limit reached - {_maxAllowedThroughput}. Cannot scale up further");
                    ScaleOffer(_dbName, _collName, Int32.Parse(_maxAllowedThroughput));
                    return;
                }
                log.Info($"Scaling up collection {_collName} throughput to {throughputIncrease}");
                ScaleOffer(_dbName, _collName, throughputIncrease);
            }
            if (isCollectionOverProvisioned)
            {
                throughputIncrease = partitionThroughput - Int32.Parse(_throughputStep);
                if (Int32.Parse(_minAllowedThroughput) > throughputIncrease)
                {
                    log.Info($"Min throughput limit reached - {_minAllowedThroughput}. Cannot scale down further");
                    ScaleOffer(_dbName, _collName, Int32.Parse(_minAllowedThroughput));
                    return;
                }
                log.Info($"Scaling down collection {_collName} throughput to {throughputIncrease}");
                ScaleOffer(_dbName, _collName, throughputIncrease);
            }
            log.Info($"Completed execution at: {DateTime.Now}");
        }

        private static string GetAccessToken()
        {
            var authenticationContext = new AuthenticationContext(string.Format("https://login.windows.net/{0}", _tenantId));
            var credential = new ClientCredential(clientId: _applicationId, clientSecret: _applicationPwd);

            var result = authenticationContext.AcquireTokenAsync(resource: "https://management.core.windows.net/", clientCredential: credential);
            result.Wait();

            if (result.Result == null)
            {
                throw new InvalidOperationException("Failed to obtain the JWT token");
            }

            return result.Result.AccessToken;
        }

        private static void ScaleOffer(string databaseName, string collectionName, int throughputIncrease)
        {
            int currentOfferThroughPut;
            int replacedOfferThroughPut;

            DocumentCollection collection = GetCollection(databaseName, collectionName);

            Offer offer = docDbClient.CreateOfferQuery().Where(o => o.ResourceLink == collection.SelfLink).AsEnumerable().Single();
            currentOfferThroughPut = ((OfferV2)offer).Content.OfferThroughput;

            docDbClient.ReplaceOfferAsync(new OfferV2(offer, throughputIncrease)).Wait();
            offer = docDbClient.CreateOfferQuery().Where(o => o.ResourceLink == collection.SelfLink).AsEnumerable().Single();
            replacedOfferThroughPut = ((OfferV2)offer).Content.OfferThroughput;
        }

        private static DocumentCollection GetCollection(string databaseName, string collectionName)
        {
            DocumentCollection collection = docDbClient.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName)).Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
            return collection;
        }

        private static int GetOfferThroughput(string collectionSelfLink)
        {
            Offer offer = docDbClient.CreateOfferQuery().Where(o => o.ResourceLink == collectionSelfLink).AsEnumerable().Single();
            return ((OfferV2)offer).Content.OfferThroughput;
        }

        private static string GetEnvironmentVariable(string name)
        {
            return System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
        }

        private static MetricCollection ReadMetricValues(string collectionSelfLink, string token)
        {
            string metricName = "Max RUs Per Second";
            string readMetric = string.Format(ConstructUriString(metricName, DateTime.UtcNow.AddHours(-1), DateTime.UtcNow.AddMinutes(-10), "PT1H", collectionSelfLink));
            var client = new HttpClient();
            client.DefaultRequestHeaders.Add("Authorization", "Bearer " + token);
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            var metricValues = client.GetStringAsync(readMetric).Result;
            MetricCollection results = JsonConvert.DeserializeObject<MetricCollection>(metricValues);
            return results;
        }

        private static string ConstructUriString(string metricName, DateTime startTime, DateTime endTime, string granularity, string collectionSelfLink)
        {
            string encodedMetricName = Uri.EscapeDataString(metricName);
            string encodedStartTime = System.Net.WebUtility.UrlEncode(startTime.ToString("yyyy-MM-ddTHH:mm:00.000Z"));
            string encodedEndTime = System.Net.WebUtility.UrlEncode(endTime.ToString("yyyy-MM-ddTHH:mm:00.000Z"));
            string encodedCollectionSelfLink = collectionSelfLink.Replace("colls", "collections").Replace("dbs", "databases");

            string resourceLink = $"{AzureManagementUri}/subscriptions/{_subscriptionId}/resourceGroups/{_resourceGroupName}/providers/Microsoft.DocumentDb/databaseAccounts/{_accountName}/{encodedCollectionSelfLink}/metrics?api-version={ApiVersion}&$filter=(name.value%20eq%20%27{encodedMetricName}%27)%20and%20endTime%20eq%20{encodedEndTime}%20and%20startTime%20eq%20{encodedStartTime}%20and%20timeGrain%20eq%20duration%27{granularity}%27";
            return resourceLink;
        }

        //
        // Summary:
        //     The collection of metric value sets.
        public class MetricCollection
        {
            //
            // Summary:
            //     Optional. Gets or sets the value of the collection.
            public IList<Metric> Value { get; set; }
        }


        //
        // Summary:
        //     Represents a metric value.
        public class MetricValue
        {
            //
            // Summary:
            //     Initializes a new instance of the MetricValue class.
            //
            // Summary:
            //     Optional. Gets or sets the average value in the time range.
            public double? Average { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the number of samples in the time range. Can be used to
            //     determine the number of values that contributed to the average value.
            public long? Count { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the last sample in the time range.
            public double? Last { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the greatest value in the time range.
            public double? Maximum { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the least value in the time range.
            public double? Minimum { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the collection of extended properties.
            public IDictionary<string, string> Properties { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the timestamp for the metric value.
            public DateTime Timestamp { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the sum of all of the values in the time range.
            public double? Total { get; set; }
        }

        //
        // Summary:
        //     A metric value set represents a set of metric values in a time period.
        public class Metric
        {
            //
            // Summary:
            //     Initializes a new instance of the Metric class.
            //
            // Summary:
            //     Optional. Gets or sets the name of the dimension.
            public string DimensionName { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the value of the dimension.
            public string DimensionValue { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the end time.
            public DateTime EndTime { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the collection of actual metric values.
            public IList<MetricValue> MetricValues { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the name and the display name of the metric.
            public LocalizableString Name { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the collection of extended properties.
            public IDictionary<string, string> Properties { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the resource ID of the resource that has emitted the metric.
            public string ResourceId { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the start time.
            public DateTime StartTime { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the time grain of the metric. Also known as the aggregation
            //     interval or frequency.
            //   public TimeSpan TimeGrain { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the unit of the metric.
            public string Unit { get; set; }
        }

        //
        // Summary:
        //     The localizable string class.
        public class LocalizableString
        {
            //
            // Summary:
            //     Optional. Gets or sets the locale specific value.
            public string LocalizedValue { get; set; }
            //
            // Summary:
            //     Optional. Gets or sets the invariant value.
            public string Value { get; set; }
        }
    }
}
