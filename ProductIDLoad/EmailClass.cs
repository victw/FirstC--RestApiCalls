using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Mail;
using System.Data.SqlClient;//for the SQL
using System.Configuration;

namespace ProductIDLoad
{
    public static class EmailClass
    {
        public static void SendEmail(string[] emailTo, string emailSubject, string emailBody)
        {
            MailMessage msg = new MailMessage();

            //loop through array of addresses
            for (int index=0; index < emailTo.Length; index++)
            { 
                msg.To.Add(emailTo[index]);
            }

            msg.From = new MailAddress(getEmailFromAddress());
            msg.Subject = emailSubject;
            msg.Body = emailBody;

            SmtpClient client = new SmtpClient(getEmailServer());

            client.Send(msg);
        }

        private static string getEmailServer()
        {
            SqlConnection GetEmailServercon = new SqlConnection();
            GetEmailServercon.ConnectionString = ConfigurationManager.ConnectionStrings["IConnectionString"].ConnectionString;
            GetEmailServercon.Open();

            //get Batch
            SqlCommand GetEmailServercommand = new SqlCommand();
            GetEmailServercommand.Connection = GetEmailServercon;
            GetEmailServercommand.CommandText = " select AppValue From Iconfiguration where appname ='ProductIDLoad' and appkey='EmailServer'";

            string EmailServer = (string)GetEmailServercommand.ExecuteScalar();
            GetEmailServercon.Close();
            //cast the int
            return EmailServer;
        }

        private static string getEmailFromAddress()
        {
            SqlConnection GetEmailFromcon = new SqlConnection();
            GetEmailFromcon.ConnectionString = ConfigurationManager.ConnectionStrings["IConnectionString"].ConnectionString;
            GetEmailFromcon.Open();

            //get Batch
            SqlCommand GetEmailFromcommand = new SqlCommand();
            GetEmailFromcommand.Connection = GetEmailFromcon;
            GetEmailFromcommand.CommandText = " select AppValue From Iconfiguration where appname ='ProductIDLoad' and appkey='FromAddress'";

            string FromAddress = (string)GetEmailFromcommand.ExecuteScalar();
            GetEmailFromcon.Close();
            //cast the int
            return FromAddress;
        }
    }
}
