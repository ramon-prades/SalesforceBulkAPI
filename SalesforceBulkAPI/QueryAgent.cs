using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

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
        private int maxTimes = 10;
        private int timeOut = 5000;
        private string baseUrl = "{instance}my.salesforce.com/services/async/44.0/job";
        private string sessionId;
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
            try
            {
                // Paso 1: Login
                Task task1 = Login();
                task1.Wait();

                // Paso 2: Crear el job
                string url = this.baseUrl;
                string content = BuildCreateJobBody();
                Task<XmlDocument> task2 = SendRequest(HttpMethod.Post, url, content, "application/xml");
                task2.Wait();
                XmlDocument doc2 = task2.Result;
                var node2 = doc2.SelectSingleNode("//*[local-name() = 'id']");
                string jobId = node2.InnerText;

                // Paso 3: Lanza la query
                url = this.baseUrl + "/" + jobId + "/batch";
                Task<XmlDocument> task3 = SendRequest(HttpMethod.Post, url, this.queryString, "text/csv");
                task3.Wait();
                XmlDocument doc3 = task3.Result;
                var node3 = doc3.SelectSingleNode("//*[local-name() = 'id']");
                string batchId = node3.InnerText;

                // Paso 4: Espera a que el job esté completo
                url = this.baseUrl + "/" + jobId + "/batch/" + batchId;
                int times = this.maxTimes;
                while (times > 0) {
                    Task<XmlDocument> task4 = SendRequest(HttpMethod.Get, url, String.Empty, "text/csv");
                    task4.Wait();
                    XmlDocument doc4 = task4.Result;
                    var node4 = doc4.SelectSingleNode("//*[local-name() = 'state']");
                    string state = node4.InnerText;
                    if (state.ToLower() == "completed")
                    {
                        times = 0;
                    } else
                    {
                        System.Threading.Thread.Sleep(this.timeOut);
                    }
                    times--;
                }

                // Paso 5: Busca el ID del resultado
                url = url + "/result";
                Task<XmlDocument> task5 = SendRequest(HttpMethod.Get, url, String.Empty, "text/csv");
                task5.Wait();
                XmlDocument doc5 = task5.Result;
                var node5 = doc5.SelectSingleNode("//*[local-name() = 'result']");
                string resultId = node5.InnerText;

                // Paso 6: Lee el resultado
                url = url + "/" + resultId;
                Task<XmlDocument> task6 = SendRequest(HttpMethod.Get, url, String.Empty, "text/csv", false);
                task6.Wait();
                XmlDocument doc6 = task5.Result;
                //var node5 = doc5.SelectSingleNode("//*[local-name() = 'result']");
                //string resultId = node5.InnerText;

                // Paso 7: Cierra el job
                url = this.baseUrl + "/" + jobId;
                content = BuildCloseJobBody();
                Task<XmlDocument> task7 = SendRequest(HttpMethod.Post, url, content, "application/xml");
                task7.Wait();
                XmlDocument doc7 = task7.Result;
                var node7 = doc7.SelectSingleNode("//*[local-name() = 'state']");            
            }
            catch (System.ApplicationException e)
            {
                throw e;
            }           
         }

        /**
        * Prepara el cliente Http
        */

        private async Task<XmlDocument> SendRequest(HttpMethod method, string url, string content, string mediaType, bool convertToXml = true)
        {
            HttpClient client = this.BuildHttpClient();
            HttpRequestMessage request = new HttpRequestMessage(method, url);
            request.Headers.Add("X-SFDC-Session", this.sessionId);
            if (!String.IsNullOrEmpty(content)) { 
                request.Content = new StringContent(content, Encoding.UTF8, mediaType);
            }
            HttpResponseMessage response = await client.SendAsync(request);
            var stream = await response.Content.ReadAsStreamAsync();
            System.Collections.Generic.IEnumerable<string> valores = response.Content.Headers.GetValues("content-type");

            StreamReader readStream = new StreamReader(stream, Encoding.UTF8);
            var result = readStream.ReadToEnd();
            var doc = new XmlDocument();
            if (convertToXml)
            {
                doc.LoadXml(result);
            } else {
                string a = "b";
            }
            if (!response.IsSuccessStatusCode)
            {
                var errNode = doc.SelectSingleNode("//*[local-name() = 'exceptionMessage']");
                throw new System.ApplicationException("Send Request: " + errNode.InnerText);
            }            
            return doc;
        }


        /**
         * STEP 1: Log in
         */
        private async Task Login()
        {
            HttpClient client = this.BuildHttpClient();
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, this.loginUrl);
            request.Headers.Add("SOAPAction", "login");
            request.Content = new StringContent(this.BuildLoginBody(), Encoding.UTF8, "text/xml");
            HttpResponseMessage response = await client.SendAsync(request);
            var stream = await response.Content.ReadAsStreamAsync();
            StreamReader readStream = new StreamReader(stream, Encoding.UTF8);
            var result = readStream.ReadToEnd();
            var doc = new XmlDocument();
            doc.LoadXml(result);
            if (!response.IsSuccessStatusCode)
            {
                var errNode = doc.SelectSingleNode("//*[local-name() = 'exceptionMessage']");
                throw new System.ApplicationException("Login Error: " + errNode.InnerText);
            }
            var node1 = doc.SelectSingleNode("//*[local-name() = 'sessionId']");
            this.sessionId = node1.InnerText;

            var node2 = doc.SelectSingleNode("//*[local-name() = 'serverUrl']");
            var instance = node2.InnerText;
            int pos = instance.IndexOf("my.salesforce.com");
            instance = instance.Substring(0, pos);

            this.baseUrl = this.baseUrl.Replace("{instance}", instance);

        } // Login

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

        private string BuildCloseJobBody()
        {
            string content = "<?xml version='1.0' encoding='UTF-8'?>";
            content += "<jobInfo xmlns='http://www.force.com/2009/06/asyncapi/dataload'>";
            content += "<state>Closed</state>";
            content += "</jobInfo>";

            return content;
        }

    }
}
