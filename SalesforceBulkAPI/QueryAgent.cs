using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace SalesforceBulkAPI
{
    // <summary>
    // Usa la BulkAPI para descargar datos
    public class QueryAgent
    {
        private WebProxy proxy = null;
        private string loginUrl = "https://login.salesforce.com/services/Soap/u/44.0";
        private string userName = "ramon.prades@gmail.com";
        private string password = "Sinosuke01";
        private string token = "AH17f4hByqxR0hZj7ctuF1WQ";

        private string createUrl = "my.salesforce.com/services/async/44.0/job";
        private string instance;
        private string sessionId;
        private string jobId;
        private string batchId;
        private string queryString;
        private string objectName;

        private HttpClient httpClient;


        public QueryAgent(string objectName, string queryString)
        {
            this.objectName = objectName;
            this.queryString = queryString;
            // curl - H "X-SFDC-Session: 00D0X000000PBHe!ARMAQGwlYkrztcgsF.MN8RrJEo8nntR_1E0YfbVus0qM7rer.pMK2rL5vWRe.gdgM18mXTQuJNMf6.JKFysizn1Nx5FX7RD7" - H "Content-Type: application/xml; charset=UTF-8" - d @create - job.xml - X POST https://ramon-prades-dev-ed.my.salesforce.com/services/async/44.0/job
        }

        public void SetProxy(string url, string userName, string password)
        {
            this.proxy = new WebProxy();
            this.proxy.Address = new Uri(url);
            this.proxy.UseDefaultCredentials = false;
            this.proxy.Credentials = new NetworkCredential(
                userName: userName,
                password: password);
        }

        public void Connect()
        {
            // Login using username and password to retrieve sessionId and serverUrl
            Task task1 = LogIn();
            task1.Wait();

            // Now creates the job
            Task task2 = CreateJob();
            task2.Wait();

            // Launches the query
            Task task3 = LaunchQuery();
            task3.Wait();

            // CHeck completed
            Task task4 = CheckCompletion();
            task4.Wait();

         }

        /**
         * STEP 1: Log in
         */
        private async Task LogIn()
        {
            HttpClient client = this.BuildHttpClient();

            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, this.loginUrl);
            request.Headers.Add("SOAPAction", "login");
            request.Content = new StringContent(this.BuildLoginBody(), Encoding.UTF8, "text/xml");

            HttpResponseMessage response = await client.SendAsync(request);
            if (!response.IsSuccessStatusCode) { 
                throw new System.ApplicationException("SOAPAction login failed with status code " + response.StatusCode.ToString());
            }

            var stream = await response.Content.ReadAsStreamAsync();
            StreamReader readStream = new StreamReader(stream, Encoding.UTF8);
            var result = readStream.ReadToEnd();
            this.parseLogInResponse(result, ref this.instance, ref this.sessionId);
        }

        /**
         * STEP 2: Create Job
         */
        private async Task CreateJob()
        {
            HttpClient client = this.BuildHttpClient();

            string url = this.instance + this.createUrl;

            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, url);
            request.Headers.Add("X-SFDC-Session", this.sessionId);
            request.Content = new StringContent(BuildCreateJobBody(), Encoding.UTF8, "application/xml");

            HttpResponseMessage response = await client.SendAsync(request);
            if (!response.IsSuccessStatusCode) {
                throw new System.ApplicationException("CreateJob failed with status code " + response.StatusCode.ToString());
            }

            var stream = await response.Content.ReadAsStreamAsync();
            StreamReader readStream = new StreamReader(stream, Encoding.UTF8);
            var result = readStream.ReadToEnd();
            this.parseCreateJobResponse(result, ref this.jobId);
        }

        /**
         * STEP 3: Launch Query
         */
        private async Task LaunchQuery()
        {
            HttpClient client = this.BuildHttpClient();

            string url = this.instance + this.createUrl + "/" + this.jobId + "/batch";

            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, url);
            request.Headers.Add("X-SFDC-Session", this.sessionId);
            request.Content = new StringContent(this.queryString, Encoding.UTF8, "text/csv");

            HttpResponseMessage response = await client.SendAsync(request);
            if (!response.IsSuccessStatusCode)
            {
                throw new System.ApplicationException("LaunchQuery failed with status code " + response.StatusCode.ToString());
            }

            var stream = await response.Content.ReadAsStreamAsync();
            StreamReader readStream = new StreamReader(stream, Encoding.UTF8);
            var result = readStream.ReadToEnd();
            this.parseLaunchQueryResponse(result, ref this.batchId);
        }

        /**
         * STEP 4: Check Completion
         */
        private async Task CheckCompletion()
        {
            HttpClient client = this.BuildHttpClient();

            string url = this.instance + this.createUrl + "/" + this.jobId + "/batch/" + this.batchId;

            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.Add("X-SFDC-Session", this.sessionId);
            //request.Content = new StringContent(this.queryString, Encoding.UTF8, "text/csv");

            HttpResponseMessage response = await client.SendAsync(request);
            if (false && !response.IsSuccessStatusCode)
            {
                throw new System.ApplicationException("LaunchQuery failed with status code " + response.StatusCode.ToString());
            }
            var stream = await response.Content.ReadAsStreamAsync();
            StreamReader readStream = new StreamReader(stream, Encoding.UTF8);
            var result = readStream.ReadToEnd();
            var state = "";
                this.parseCheckCompletionResponse(result, ref state);
        }



        private void parseLogInResponse(string xmlString, ref string instance, ref string sessionId)
        {
            var doc = new XmlDocument();
            doc.LoadXml(xmlString);

            var ns = new XmlNamespaceManager(doc.NameTable);
            ns.AddNamespace("force", "urn:partner.soap.sforce.com");
            ns.AddNamespace("soapenv", "http://schemas.xmlsoap.org/soap/envelope/");

            XmlNode serverNode = doc.SelectSingleNode("//force:serverUrl", ns);
            if (serverNode != null)
            {
                string serverUrl = serverNode.InnerText;
                int pos = serverUrl.IndexOf("my.salesforce.com");
                instance = serverUrl.Substring(0, pos);
            }

            XmlNode sessionNode = doc.SelectSingleNode("//force:sessionId", ns);
            if (sessionNode != null) sessionId = sessionNode.InnerText;

        }

        private void parseCreateJobResponse(string xmlString, ref string jobId)
        {
            var doc = new XmlDocument();
            doc.LoadXml(xmlString);

            var ns = new XmlNamespaceManager(doc.NameTable);
            ns.AddNamespace("api", "http://www.force.com/2009/06/asyncapi/dataload");

            XmlNode node= doc.SelectSingleNode("//api:jobInfo/api:id", ns);
            if (node != null)
            {
                jobId = node.InnerText;
            }
        }

        private void parseLaunchQueryResponse(string xmlString, ref string batchId)
        {
            var doc = new XmlDocument();
            doc.LoadXml(xmlString);

            var ns = new XmlNamespaceManager(doc.NameTable);
            ns.AddNamespace("api", "http://www.force.com/2009/06/asyncapi/dataload");

            XmlNode node = doc.SelectSingleNode("//api:batchInfo/api:id", ns);
            if (node != null)
            {
                batchId = node.InnerText;
            }
        }

        private void parseCheckCompletionResponse(string xmlString, ref string state)
        {
            var doc = new XmlDocument();
            doc.LoadXml(xmlString);

            var ns = new XmlNamespaceManager(doc.NameTable);
            ns.AddNamespace("api", "http://www.force.com/2009/06/asyncapi/dataload");

            XmlNode node = doc.SelectSingleNode("//api:batchInfo/api:state", ns);
            if (node != null)
            {
                state = node.InnerText;
            }
        }


        private HttpClient BuildHttpClient()
        {
            if (this.httpClient == null)
            {
                var httpClientHandler = new HttpClientHandler();
                httpClientHandler.Proxy = this.proxy;
                httpClientHandler.PreAuthenticate = true;
                httpClientHandler.UseDefaultCredentials = false;
                this.httpClient = new HttpClient(handler: httpClientHandler, disposeHandler: true);
            }
            return this.httpClient;
        }

        private string BuildLoginBody()
        {
            string content = "<?xml version = '1.0' encoding = 'utf-8'?>";
            content += "<env:Envelope xmlns:xsd = 'http://www.w3.org/2001/XMLSchema' xmlns:xsi = 'http://www.w3.org/2001/XMLSchema-instance' xmlns:env='http://schemas.xmlsoap.org/soap/envelope/'>";
            content += "<env:Body>";
            content += "<n1:login xmlns:n1='urn:partner.soap.sforce.com'>";
            content += "<n1:username>" + this.userName + "</n1:username>";
            content += "<n1:password>" + this.password + this.token + "</n1:password>";
            content += "</n1:login></env:Body></env:Envelope>";

            return content;
        }

        private string BuildCreateJobBody() {
            string content = "<?xml version = '1.0' encoding = 'UTF-8'?>";
            content += "<jobInfo xmlns='http://www.force.com/2009/06/asyncapi/dataload'>";
            content += "<operation>query</operation>";
            content += "<object>" + this.objectName + "</object>";
            content += "<concurrencyMode>Parallel</concurrencyMode>";
            content += "<contentType>CSV</contentType>";
            content += "</jobInfo>";

            return content;
        }


    }
}
