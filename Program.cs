using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Utilities.SQLServer;

namespace SQLServerScripts
{
    class Program
    {
        static void Main(string[] args)
        {
            SQLServerScripter srv = new SQLServerScripter();
            srv.LogMessage += LogMessage;

            //
            // Setup the connection details
            // NB: If you do not set SQLServerScripterConnection.User the connection will use
            // ActiveDirectory credentials
            //
            SQLServerScripterConnection c = new SQLServerScripterConnection();
            c.Server = "MYSQLSRV01";

            // c.User = "MyUser";
            // c.Password = "MyPassword";

            //
            // List of databases to exclude from scripting
            // NB: System databases are automatically excluded
            //
            List<string> DBExclusions = new List<string> { "DBName1", "DBName2" };

            //
            // Create the scripts
            //
            srv.ScriptEverything(c, DBExclusions);
            System.Environment.ExitCode = 0;
        }

        static void LogMessage(object sender, SQLServerScripterMessageArgs m)
        {
            Console.WriteLine("----");
            Console.WriteLine(String.Format("Server: {0}", m.Server));
            if (m.Database != null)
                Console.WriteLine(String.Format("Database: {0}", m.Database));

            Console.WriteLine(String.Format("Object: {0}.{1}", m.ObjectType, m.ObjectName));
            Console.WriteLine(String.Format("Output File: {0}", m.Path));
        }
    }
}
