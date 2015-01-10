//
//Programmer: V Wilson
//Create Date: 12/17/14

//  Description: 
//  It reads all the skus that do not have product id's  and 
//  calls the API retrieving the product id 
//  The initial load was taking a few hours.  Tasks are used to allow concurrent processing of skus
//  Tasks create threads - the semaphore is used to throttle the # of tasks/threads.  
//  We currently set the limit at 4
//  Please note the retry logic on the api - the api sometimes provides a 500 response - this is their internal error
//  We want to retry on these
//  We also want to retry on a 429 

//*******************************************************************************
//Modifications
//
//Number      Date      Initials  Description
//
//******************************************************************************* 
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;//for the SQL
using System.Net;//for webclient
using Newtonsoft.Json;//for Jsonconvert http://www.nuget.org/packages/Newtonsoft.Json
using System.Configuration;
using System.Threading;

namespace ProductIDLoad
{
    class Program
    {
        //globals 
        static SqlConnection con2 = new SqlConnection();
        static Int32 batchIdNum;
        static String errorMessage;
        static String Attempt;
        static String MethodName;
        static String apiPath;

        static void Main(string[] args)
        {

            Setup();

            ReadFeed();

            // Write Batch Upd
            insertRowBatch("END", " ", batchIdNum);

            SendErrorsEmail();

            con2.Close();

        }

        private static void SendErrorsEmail()
        {
            //var array emailTo 
            string[] Emailaddresses = getEmailAddresses();
            int BatchErrors = getErrorCount();
            if (BatchErrors != 0)
            {
                string EmailSubject = "ProductIDLoad Status " + BatchErrors.ToString() + " - Errors";
                String EmailBody = "ProductIDLoadProcess has " + BatchErrors.ToString() + " Errors.  Please see ErrorLogTB";
                EmailClass.SendEmail(Emailaddresses, EmailSubject, EmailBody);
            }
        }

        private static int getErrorCount()
        {
            SqlConnection ErrorCountcon = new SqlConnection();
            ErrorCountcon.ConnectionString = ConfigurationManager.ConnectionStrings["IConnectionString"].ConnectionString;
            ErrorCountcon.Open();

            //get Batch
            SqlCommand GetErrorscommand = new SqlCommand();
            GetErrorscommand.Connection = ErrorCountcon;
            GetErrorscommand.CommandText = "  select count(*) from errorLogTB where errorMessage<>'No Data: True 200 ok' and batchIdNum=@BatchID";
            GetErrorscommand.Parameters.AddWithValue("@BatchID", batchIdNum);

            Int32 TotalErrors = (Int32)GetErrorscommand.ExecuteScalar();
            ErrorCountcon.Close();
            //cast the int
            return TotalErrors;
        }

        private static string[] getEmailAddresses()
        {
            //use a list and convert it to an array because c# does not have dynamic arrays
            var emailAddressesList = new List<string>();

            SqlConnection EmailCon = new SqlConnection();
            EmailCon.ConnectionString = ConfigurationManager.ConnectionStrings["IConnectionString"].ConnectionString;
            EmailCon.Open();

            //get Batch
            SqlCommand EmailSqlcommand = new SqlCommand();
            EmailSqlcommand.Connection = EmailCon;
            EmailSqlcommand.CommandText = "Select AppValue From IConfiguration Where AppName='ProductIDLoad' and AppKey = 'EmailList'";

            SqlDataReader reader = EmailSqlcommand.ExecuteReader();

            // Call Read before accessing data. 
            while (reader.Read())
            {
                emailAddressesList.Add(reader.GetString(0));
            }

            EmailCon.Close();

            string[] EmailAddressArray;
            EmailAddressArray = emailAddressesList.ToArray();
            return EmailAddressArray;
        }

        private static void Setup()
        {
            //Open the SQL connection - get configuration info
            //Get Batch Id - 
            //Write the Begin record
            //Set up shared connection
            //SqlConnection con2 = new SqlConnection();
            con2.ConnectionString = ConfigurationManager.ConnectionStrings["IConnectionString"].ConnectionString;
            con2.Open();

            //get Batch
            SqlCommand GetBatchIDcommand = new SqlCommand();
            GetBatchIDcommand.Connection = con2;
            GetBatchIDcommand.CommandText = "Select Next Value for BatchIdSequence";

            //cast the int
            batchIdNum = (Int32)GetBatchIDcommand.ExecuteScalar();

            insertRowBatch("BEGIN", "ProductsIdLoad", batchIdNum);

            //get Batch
            SqlCommand GetApiUrlcommand = new SqlCommand();
            GetApiUrlcommand.Connection = con2;
            GetApiUrlcommand.CommandText = "select appvalue from IConfiguration Where AppName = 'ProductIDLoad'	and AppKey = 'ApiUrl'";
            //cast to a string
            apiPath = (string)GetApiUrlcommand.ExecuteScalar();
        }

        private static void ReadFeed()
        {

            //Object of type task controls the threading
            Task SkuTask;
            var SkuTasksList = new List<Task>();

            var totalApiCalls = 0;

            SqlCommand cmd2 = new SqlCommand();
            cmd2.Connection = con2;
            cmd2.CommandText = "select sSku from dbo.productstb where StatusFlag = 'A' and  id = ' ' ";
            SqlDataReader reader = cmd2.ExecuteReader();


            // Call Read before accessing data. 
            while (reader.Read())
            {
                totalApiCalls++;
                string Sku = reader.GetString(0);

                //returns a task 
                SkuTask = ProcessSku(Sku);
                SkuTasksList.Add(SkuTask);

            }

            //Need to wait for all the tasks before finishing
            //TODO The tasklist at this point includes all the tasks even though most of them have finished - I'm not sure how to clean it up??
            Task.WaitAll(SkuTasksList.ToArray());

            //Write Batch Update
            var eventType = "API_COUNT";
            var comments2 = totalApiCalls.ToString();
            insertRowBatch(eventType, comments2, batchIdNum);

        }

        static Task ProcessSku(string Sku)
        {
            //TODO configure the number of threads so it's not hard-coded
            // The semaphore contols the number of tasks/threads created
            var semaphore = new SemaphoreSlim(4);

            // This creates the task/thread all the code in the curly braces{} after the => is run in the task
            Task SkuTask = Task.Run(() =>
            {
                //the semaphore wait until it a thread is available
                semaphore.Wait();
                try
                {
                    // Call the API 

                    var apiCall = string.Format(apiPath, Sku.Trim());

                    var apiData = _download_serialized_json_data<JsonData>(Sku, apiCall);

                    if (apiData.status == 200)
                    {
                        if (apiData.response.noData)
                        {
                            // Write Error Log
                            errorMessage = "No Data: " + apiData.response.noData.ToString() + " " + apiData.status.ToString() + " " + apiData.message;
                            MethodName = System.Reflection.MethodBase.GetCurrentMethod().Name;
                            insertRowErrorlog(Sku, batchIdNum, apiCall, errorMessage, " ", MethodName);
                        }
                        else
                        {
                            UpdateTheSku(Sku, apiData);
                        }
                    }
                    else
                    {
                        if (apiData.status != 0)
                        {
                            // Write Error Log
                            errorMessage = "Status: " + apiData.status + " Message " + apiData.message;
                            MethodName = System.Reflection.MethodBase.GetCurrentMethod().Name;
                            insertRowErrorlog(Sku, batchIdNum, apiCall, errorMessage, " ", MethodName);
                        }
                    }
                }
                finally
                {
                    //this releases the semaphore - think of it as passing the baton
                    semaphore.Release();
                }
            });
            return SkuTask;
        }

        private static void UpdateTheSku(string Sku, JsonData apiData)
        {

            SqlCommand cmd = new SqlCommand();
            cmd.Connection = con2;
            cmd.CommandText = "update dbo.productstb set id=@id, title=@title, brandId=@brandId, brandName=@brandName, categoryIdPath=@categoryIdPath, " +
                                "categoryNamePath=@categoryNamePath, imageUrl=@imageUrl, upc=@upc, mpn=@mpn, storeCount=@storeCount, offersCount=@offersCount, offersPriceRange=@offersPriceRange," +
                                " updateTimeStamp=@updateTimeStamp" +
                              " where sSku = @Sku";
            cmd.Parameters.AddWithValue("@id", apiData.response.products[0].id);
            cmd.Parameters.AddWithValue("@title", apiData.response.products[0].title);
            cmd.Parameters.AddWithValue("@brandId", apiData.response.products[0].brandId);
            cmd.Parameters.AddWithValue("@brandName", apiData.response.products[0].brandName);
            cmd.Parameters.AddWithValue("@categoryIdPath", apiData.response.products[0].categoryIdPath);
            cmd.Parameters.AddWithValue("@categoryNamePath", apiData.response.products[0].categoryNamePath);
            cmd.Parameters.AddWithValue("@imageUrl", apiData.response.products[0].imageUrl);
            cmd.Parameters.AddWithValue("@upc", apiData.response.products[0].upc);
            cmd.Parameters.AddWithValue("@mpn", apiData.response.products[0].mpn);
            cmd.Parameters.AddWithValue("@storeCount", apiData.response.products[0].storeCount);
            cmd.Parameters.AddWithValue("@offersCount", apiData.response.products[0].offersCount);
            cmd.Parameters.AddWithValue("@offersPriceRange", apiData.response.products[0].offersPriceRange);
            cmd.Parameters.AddWithValue("@updateTimeStamp", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"));
            cmd.Parameters.AddWithValue("@Sku", Sku);
            try
            {
                int NbrOfRows = cmd.ExecuteNonQuery();
                if (NbrOfRows == 0)
                {
                    Attempt = cmd.CommandText;
                    MethodName = System.Reflection.MethodBase.GetCurrentMethod().Name;
                    insertRowErrorlog(Sku, batchIdNum, Attempt, "SQL Statement failed", " ", MethodName);
                }

            }
            catch (Exception ex)
            {
                Attempt = cmd.CommandText;
                errorMessage = "SQL Statement failed" + ex.Message;
                MethodName = System.Reflection.MethodBase.GetCurrentMethod().Name;
                insertRowErrorlog(Sku, batchIdNum, Attempt, errorMessage, "", MethodName);
            }
        }

        private static T _download_serialized_json_data<T>(string Sku, string url) where T : new()
        {
            var apistatus = "0";
            var json_data = string.Empty;
            const int MaxAttempts = 3;
            int attempt = 0;

            using (var w = new WebClient())
            {
                // attempt to download JSON data as a string
                while (++attempt <= MaxAttempts)
                {
                    try
                    {
                        json_data = w.DownloadString(url);
                    }
                    catch (WebException ex)
                    {
                        // Write Error Log
                        var apiUrl = url;
                        //we only want to attempt again on a 429 or 500
                        if (ex.Status == WebExceptionStatus.ProtocolError)
                        {
                            HttpWebResponse response = (HttpWebResponse)ex.Response;
                            apistatus = response.StatusCode.ToString();
                        }
                        //List of status codes http://msdn.microsoft.com/en-us/library/ee434093.aspx
                        if (apistatus == "InternalServerError" || apistatus == "Conflict")
                        {
                            //If third attempt write the message
                            if (attempt == 3)
                            {
                                var errorMessage = "Status " + apistatus + ":Message " + ex.Message;
                                MethodName = System.Reflection.MethodBase.GetCurrentMethod().Name;
                                insertRowErrorlog(Sku, batchIdNum, apiUrl, errorMessage, " ", MethodName);
                            }
                            else
                            {
                                //? quarter of a minute
                                Thread.Sleep(2500);
                            }
                        }
                        else
                        {
                            //Test for connection closed - retry if you get it - don't need to sleep on these
                            if ((ex.Status == WebExceptionStatus.PipelineFailure ||
                                ex.Status == WebExceptionStatus.Timeout||
                                ex.Status == WebExceptionStatus.RequestCanceled)
                                && attempt < 3)
                            {
                                //do nothing - let it try three times
                                Console.WriteLine("Retry");
                            }
                            else
                            {
                                //write the message for any other error
                                var errorMessage = "WebException " + ex.Status + ":Message " + ex.Message + " Attempts: " + attempt;
                                MethodName = System.Reflection.MethodBase.GetCurrentMethod().Name;
                                insertRowErrorlog(Sku, batchIdNum, apiUrl, errorMessage, " ", MethodName);
                                break;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Write Error Log
                        //Need a way to pick up the sku
                        var apiUrl = url;
                        errorMessage = "Message " + ex.Message;
                        MethodName = System.Reflection.MethodBase.GetCurrentMethod().Name;
                        insertRowErrorlog(Sku, batchIdNum, apiUrl, errorMessage, " ", MethodName);
                    }
                    if (json_data != "")
                    {
                        break;
                    }
                }
            }

            // if string with JSON data is not empty, deserialize it to class and return its instance 
            return !string.IsNullOrEmpty(json_data) ? JsonConvert.DeserializeObject<T>(json_data) : new T();

        }

        private static void insertRowBatch(string eventType, string comments, int batchIdNum)
        {

            SqlCommand cmd = new SqlCommand();
            cmd.Connection = con2;
            cmd.CommandText = "INSERT INTO  dbo.batchHistorytb (batchIdNum, eventType, applicationProcess, comments ) values(@batchIdNum, @eventType, 'ProductIDLoad', @comments)";
            cmd.Parameters.AddWithValue("@batchIdNum", batchIdNum);
            cmd.Parameters.AddWithValue("@eventType", eventType);
            cmd.Parameters.AddWithValue("@comments", comments);
            int NbrOfRows = cmd.ExecuteNonQuery();
            if (NbrOfRows != 1)
            {
                Attempt = cmd.CommandText;
                MethodName = System.Reflection.MethodBase.GetCurrentMethod().Name;
                insertRowErrorlog("No Sku", batchIdNum, Attempt, "SQL Statement failed", "Does this work - writing an error on the batch insert", MethodName);
            }
        }

        private static void insertRowErrorlog(string sSku, int batchIdNum, string apiUrl, string errorMessage, string comments, string MethodName)
        {
            //I pulled out the try/catch here - if we can't write an error we might want to crash

            SqlCommand cmd = new SqlCommand();
            cmd.Connection = con2;
            cmd.CommandText = "INSERT INTO  dbo.errorLogTB (sSku, batchIdNum, apiUrl, errorMessage, comments, applicationProcess, methodWithError) values(@sSku, @batchIdNum, @apiUrl, @errorMessage, @comments, 'ProductsIDLoad', @MethodName)";
            cmd.Parameters.AddWithValue("@sSku", sSku);
            cmd.Parameters.AddWithValue("@batchIdNum", batchIdNum);
            cmd.Parameters.AddWithValue("@apiUrl", apiUrl);
            cmd.Parameters.AddWithValue("@errorMessage", errorMessage);
            cmd.Parameters.AddWithValue("@comments", comments);
            cmd.Parameters.AddWithValue("@MethodName", MethodName);
            //NbrOfRows Here is mostly for debugging purposes
            //Other option is to throw an exception and force a hard halt if we can't insert an error record???
            int NbrOfRows = cmd.ExecuteNonQuery();

        }


        public class JsonData
        {


            public int status { get; set; }
            public string message { get; set; }
            public Request request { get; set; }
            public Response response { get; set; }

            public class Request
            {
                public string storeId { get; set; }
                public string pageNumber { get; set; }
                public string app_id { get; set; }
                public string app_key { get; set; }
            }

            public class Response
            {
                public int count { get; set; }
                public Product[] products { get; set; }
                public int pageNumber { get; set; }
                public bool noData { get; set; }
            }

            public class Product
            {
                public string id { get; set; }
                public string title { get; set; }
                public int brandId { get; set; }
                public string brandName { get; set; }
                public string categoryIdPath { get; set; }
                public string categoryNamePath { get; set; }
                public string imageUrl { get; set; }
                public string upc { get; set; }
                public string mpn { get; set; }
                public int storeCount { get; set; }
                public int offersCount { get; set; }
                public string offersPriceRange { get; set; }
            }
        }
    }
}
