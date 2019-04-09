using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

namespace CosmosAutoScale
{
    public static class ArtificialTraffic
    {
        [FunctionName("ArtificialTraffic")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request.");

            // parse query parameter
            string records = req.GetQueryNameValuePairs()
                .FirstOrDefault(q => string.Compare(q.Key, "records", true) == 0)
                .Value;

            // how many new records?
            int recs = 0;
            if(!int.TryParse(records, out recs))
            {
                recs = 1000;
            }
            for (int i = 0; i < recs; i++)
            {
                // add a record..

            }
            
            //return name == null
            //    ? req.CreateResponse(HttpStatusCode.BadRequest, "Please pass a name on the query string or in the request body")
                //: req.CreateResponse(HttpStatusCode.OK, "Hello " + name);
                return req.CreateResponse(HttpStatusCode.OK, "Success.");
        }
    }
}
