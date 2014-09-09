using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.IO;

using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Smo.Agent;
using Microsoft.SqlServer.Management.Common;

namespace Utilities.SQLServer
{
    /// <summary>
    /// SQL Server Object Types
    /// </summary>
    public enum SQLServerScripterType
    {
        Database,
        Login,
        SQLAgentJob,
        User,
        Schema,
        DatabaseRole,
        ApplicationRole,
        Table,
        View,
        Proc,
        Function,
        Synonym,
        UserType,
        UserDataType,
        UserTableType
    }

    /// <summary>
    /// SQLServerScripter Connection Details
    /// </summary>
    public class SQLServerScripterConnection
    {
        public string Server { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
    }

    /// <summary>
    /// SQLServerScripterMessageArgs - Used for the LogMessage Event
    /// </summary>
    public class SQLServerScripterMessageArgs : EventArgs
    {
        public string Server { get; set; }
        public string Database { get; set; }
        public string ObjectName { get; set; }
        public string Path { get; set; }
        public SQLServerScripterType ObjectType { get; set; }

        public SQLServerScripterMessageArgs(string server, string database, string objectname, string path, SQLServerScripterType st)
        {
            Server = server;
            Database = database;
            ObjectName = objectname;
            Path = path;
            ObjectType = st;
        }
    }

    /// <summary>
    /// SQLServerScripter class
    /// </summary>
    public class SQLServerScripter
    {
        public event EventHandler<SQLServerScripterMessageArgs> LogMessage;

        private ScriptingOptions m_ScriptingOptions;
        private string m_Server;
        private List<string> m_DBExceptionList = new List<string> { "master", "model", "msdb", "tempdb", "ReportServer", "ReportServerTempDB" };

        public SQLServerScripter()
        {
            //
            // Setup the SQL scripting options
            //
            m_ScriptingOptions = new ScriptingOptions();
            m_ScriptingOptions.IncludeDatabaseContext = true;
            m_ScriptingOptions.EnforceScriptingOptions = true;
            m_ScriptingOptions.NoCommandTerminator = false;
            m_ScriptingOptions.ToFileOnly = true;
            m_ScriptingOptions.ContinueScriptingOnError = true;
            m_ScriptingOptions.Indexes = true;
            m_ScriptingOptions.IncludeDatabaseRoleMemberships = true;
            m_ScriptingOptions.Permissions = true;
            m_ScriptingOptions.Triggers = true;

            m_ScriptingOptions.Encoding = Encoding.UTF8;
        }

        /// <summary>
        /// Raise a log message event
        /// </summary>
        /// <param name="database">Datebase Name</param>
        /// <param name="objectname">Object Name</param>
        /// <param name="path">Path to output file</param>
        /// <param name="st">Object Type</param>
        protected void SendLogMessage(string database, string objectname, string path, SQLServerScripterType st)
        {
            if (LogMessage != null)
                LogMessage(this, new SQLServerScripterMessageArgs(m_Server, database, objectname, path, st));
        }

        /// <summary>
        /// Script all objects on the server
        /// </summary>
        /// <param name="c">Connection Details</param>
        public void ScriptEverything(SQLServerScripterConnection c)
        {
            ScriptEverything(c, new List<string> { });
        }

        /// <summary>
        /// Script all objects on the server excluding the defined databases
        /// </summary>
        /// <param name="c">Connection Details</param>
        /// <param name="ExcludeDB">List of Databases ot exclude</param>
        public void ScriptEverything(SQLServerScripterConnection c, List<string> ExcludeDB)
        {
            if (ExcludeDB == null)
                throw new Exception("ExcludeDB cannot be null");

            m_Server = c.Server;

            var Excludes = m_DBExceptionList.Concat(ExcludeDB);

            //
            // Setup connection
            //
            ServerConnection conn = new ServerConnection();

            if (c.User != null)
            {
                conn.LoginSecure = false;
                conn.Login = c.User;
                conn.Password = c.Password;
            }
            else
                conn.LoginSecure = true;

            conn.ServerInstance = m_Server;

            Server srv = new Server(conn);

            ScriptLogins(srv.Logins);
            ScriptSQLAgentJobs(srv.JobServer);

            var databases = from Database db in srv.Databases
                            where !Excludes.Contains(db.Name)
                            select db;


            foreach (Database d in databases)
            {
                ScriptDatabase(d);
                ScriptUsers(d);
                ScriptSchemas(d);
                ScriptDatabaseRoles(d);
                ScriptApplicationRoles(d);
                ScriptTables(d);
                ScriptViews(d);
                ScriptProcs(d);
                ScriptFunctions(d);
                ScriptSynonyms(d);
                ScriptTypes(d);
                ScriptDataTypes(d);
                ScriptTableTypes(d);
            }

        }

        /// <summary>
        /// Script the CREATE DATABASE command
        /// </summary>
        /// <param name="d">Database</param>
        private void ScriptDatabase(Database d)
        {
            string path = String.Format(@"{0}\Databases\{1}", m_Server, d.Name);

            if (Directory.Exists(path) == false)
                Directory.CreateDirectory(path);
            else
                DeleteAllSQLFiles(path);

            m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(d.Name.ToString()));
            SendLogMessage(d.Name, d.Name, m_ScriptingOptions.FileName, SQLServerScripterType.Database);
            d.Script(m_ScriptingOptions);
        }

        /// <summary>
        /// Script the Database Roles
        /// </summary>
        /// <param name="d">Database</param>
        private void ScriptDatabaseRoles(Database d)
        {
            string path = String.Format(@"{0}\Databases\{1}\Roles - Database", m_Server, d.Name);
            int files = 0;

            if (d.Roles.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                foreach (DatabaseRole dbrole in d.Roles)
                {
                    if (dbrole.IsFixedRole == false && dbrole.Name != "public")
                    {


                        //
                        // Create the script
                        //
                        m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(dbrole.Name.ToString()));
                        SendLogMessage(d.Name, dbrole.Name, m_ScriptingOptions.FileName, SQLServerScripterType.DatabaseRole);
                        dbrole.Script(m_ScriptingOptions);
                        files++;
                    }
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }

        /// <summary>
        /// Script the Application Roles
        /// </summary>
        /// <param name="d">Database</param>
        private void ScriptApplicationRoles(Database d)
        {
            string path = String.Format(@"{0}\Databases\{1}\Roles - Application", m_Server, d.Name);
            int files = 0;

            if (d.Roles.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                foreach (ApplicationRole dbrole in d.ApplicationRoles)
                {
                    //
                    // Create the script
                    //
                    m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(dbrole.Name.ToString()));
                    SendLogMessage(d.Name, dbrole.Name, m_ScriptingOptions.FileName, SQLServerScripterType.ApplicationRole);
                    dbrole.Script(m_ScriptingOptions);
                    files++;
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }

        /// <summary>
        /// Script the Tables
        /// </summary>
        /// <param name="d">Database</param>
        private void ScriptTables(Database d)
        {
            string path = String.Format(@"{0}\Databases\{1}\Tables", m_Server, d.Name);
            int files = 0;

            if (d.Tables.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                foreach (Table table in d.Tables)
                {
                    if (table.IsSystemObject == false && table.Name[table.Name.Length - 1] != '$')
                    {
                        //
                        // Create the script
                        //
                        m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(table.Name.ToString()));
                        SendLogMessage(d.Name, table.Name, m_ScriptingOptions.FileName, SQLServerScripterType.Table);
                        table.Script(m_ScriptingOptions);
                        files++;
                    }
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }

        /// <summary>
        /// Script the Views
        /// </summary>
        /// <param name="d">Database</param>
        private void ScriptViews(Database d)
        {
            string path = String.Format(@"{0}\Databases\{1}\Views", m_Server, d.Name);
            int files = 0;

            if (d.Views.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                foreach (View view in d.Views)
                {
                    if (view.IsSystemObject == false)
                    {
                        //
                        // Create the script
                        //
                        m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(view.Name.ToString()));
                        SendLogMessage(d.Name, view.Name, m_ScriptingOptions.FileName, SQLServerScripterType.View);
                        view.Script(m_ScriptingOptions);
                        files++;
                    }
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }

        /// <summary>
        /// Script the Stored Procedures
        /// </summary>
        /// <param name="d">Database</param>
        private void ScriptProcs(Database d)
        {
            string path = String.Format(@"{0}\Databases\{1}\Procs", m_Server, d.Name);
            int files = 0;

            if (d.StoredProcedures.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                foreach (StoredProcedure proc in d.StoredProcedures)
                {
                    if (proc.IsSystemObject == false && proc.IsEncrypted == false)
                    {
                        //
                        // Create the script
                        //
                        m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(proc.Name.ToString()));
                        SendLogMessage(d.Name, proc.Name, m_ScriptingOptions.FileName, SQLServerScripterType.Proc);
                        proc.Script(m_ScriptingOptions);
                        files++;
                    }
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }

        /// <summary>
        /// Script the Functions
        /// </summary>
        /// <param name="d">Database</param>
        private void ScriptFunctions(Database d)
        {
            string path = String.Format(@"{0}\Databases\{1}\Functions", m_Server, d.Name);
            int files = 0;

            if (d.UserDefinedFunctions.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                foreach (UserDefinedFunction function in d.UserDefinedFunctions)
                {
                    if (function.IsSystemObject == false && function.IsEncrypted == false)
                    {
                        //
                        // Create the script
                        //
                        m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(function.Name.ToString()));
                        SendLogMessage(d.Name, function.Name, m_ScriptingOptions.FileName, SQLServerScripterType.Function);
                        function.Script(m_ScriptingOptions);
                        files++;
                    }
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }

        /// <summary>
        /// Script the Synonyms
        /// </summary>
        /// <param name="d">Database</param>
        private void ScriptSynonyms(Database d)
        {
            string path = String.Format(@"{0}\Databases\{1}\Synonyms", m_Server, d.Name);
            int files = 0;

            if (d.Synonyms.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                foreach (Synonym syn in d.Synonyms)
                {
                    //
                    // Create the script
                    //
                    m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(syn.Name.ToString()));
                    SendLogMessage(d.Name, syn.Name, m_ScriptingOptions.FileName, SQLServerScripterType.Synonym);
                    syn.Script(m_ScriptingOptions);
                    files++;
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }

        /// <summary>
        /// Script the User Defined Types
        /// </summary>
        /// <param name="d">Database</param>
        private void ScriptTypes(Database d)
        {
            string path = String.Format(@"{0}\Databases\{1}\Types\User-Defined Types", m_Server, d.Name);
            int files = 0;

            if (d.UserDefinedTypes.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                foreach (UserDefinedType t in d.UserDefinedTypes)
                {
                    //
                    // Create the script
                    //
                    m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(t.Name.ToString()));
                    SendLogMessage(d.Name, t.Name, m_ScriptingOptions.FileName, SQLServerScripterType.UserType);
                    t.Script(m_ScriptingOptions);
                    files++;
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }

        /// <summary>
        /// Script the User Defined Data Types
        /// </summary>
        /// <param name="d">Database</param>
        private void ScriptDataTypes(Database d)
        {
            string path = String.Format(@"{0}\Databases\{1}\Types\User-Defined Data Types", m_Server, d.Name);
            int files = 0;

            if (d.UserDefinedDataTypes.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                foreach (UserDefinedDataType t in d.UserDefinedDataTypes)
                {
                    //
                    // Create the script
                    //
                    m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(t.Name.ToString()));
                    SendLogMessage(d.Name, t.Name, m_ScriptingOptions.FileName, SQLServerScripterType.UserDataType);
                    t.Script(m_ScriptingOptions);
                    files++;
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }


        /// <summary>
        /// Script the User Defined Table Types
        /// </summary>
        /// <param name="d">Database</param>
        private void ScriptTableTypes(Database d)
        {
            string path = String.Format(@"{0}\Databases\{1}\Types\User-Defined Table Types", m_Server, d.Name);
            int files = 0;

            if (d.UserDefinedTableTypes.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                foreach (UserDefinedTableType t in d.UserDefinedTableTypes)
                {
                    //
                    // Create the script
                    //
                    m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(t.Name.ToString()));
                    SendLogMessage(d.Name, t.Name, m_ScriptingOptions.FileName, SQLServerScripterType.UserTableType);
                    t.Script(m_ScriptingOptions);
                    files++;
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }

        /// <summary>
        /// Script the Schemas
        /// </summary>
        /// <param name="d">Database</param>
        private void ScriptSchemas(Database d)
        {
            string path = String.Format(@"{0}\Databases\{1}\Schemas", m_Server, d.Name);
            int files = 0;

            if (d.Schemas.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                foreach (Schema t in d.Schemas)
                {
                    if (t.IsSystemObject == false)
                    {
                        //
                        // Create the script
                        //
                        m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(t.Name.ToString()));
                        SendLogMessage(d.Name, t.Name, m_ScriptingOptions.FileName, SQLServerScripterType.Schema);
                        t.Script(m_ScriptingOptions);
                        files++;
                    }
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }

        /// <summary>
        /// Script the Users
        /// </summary>
        /// <param name="d">Database</param>
        private void ScriptUsers(Database d)
        {
            string path = String.Format(@"{0}\Databases\{1}\Users", m_Server, d.Name);
            int files = 0;

            if (d.Users.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                foreach (User user in d.Users)
                {
                    if (user.IsSystemObject == false)
                    {
                        //
                        // Create the script
                        //
                        m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(user.Name.ToString()));
                        SendLogMessage(d.Name, user.Name, m_ScriptingOptions.FileName, SQLServerScripterType.User);
                        user.Script(m_ScriptingOptions);
                        files++;
                    }
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }

        /// <summary>
        /// Script the Server Logins
        /// </summary>
        /// <param name="l">LoginCollection</param>
        private void ScriptLogins(LoginCollection l)
        {
            string path = String.Format(@"{0}\Logins", m_Server);
            int files = 0;

            if (l.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                //
                // Loop over all the SQL Agent jobs
                //
                foreach (Login login in l)
                {
                    //
                    // Create the script
                    //
                    m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(login.Name.ToString()));
                    SendLogMessage(null, login.Name, m_ScriptingOptions.FileName, SQLServerScripterType.Login);
                    login.Script(m_ScriptingOptions);
                    Login_FixupRandomPassword(m_ScriptingOptions.FileName);
                    files++;
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }

        /// <summary>
        /// Changes the randomly generated password in a CREATE LOGIN script to '**CHANGEME**'
        /// </summary>
        /// <param name="filename">Script file to fixup</param>
        private void Login_FixupRandomPassword(string filename)
        {
            string newfilepath = String.Format("{0}.new", filename);
            var wholefile = File.ReadAllText(filename);
            using (FileStream newfile = new FileStream(newfilepath, FileMode.Create, FileAccess.Write))
            {
                using (StreamWriter w = new StreamWriter(newfile, Encoding.UTF8))
                {
                    w.WriteLine(Regex.Replace(wholefile, "WITH PASSWORD=N'.+', DEFAULT", "WITH PASSWORD=N'**CHANGEME**', DEFAULT", RegexOptions.Singleline));

                    w.Flush();
                }
            }
            File.Delete(filename);
            File.Move(newfilepath, filename);
        }

        /// <summary>
        /// Script the SQLAgent Jobs
        /// </summary>
        /// <param name="j">JobServer</param>
        private void ScriptSQLAgentJobs(JobServer j)
        {
            string path = String.Format(@"{0}\SQLAgent", m_Server);
            int files = 0;

            if (j.Jobs.Count > 0)
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);
                else
                    DeleteAllSQLFiles(path);

                //
                // Loop over all the SQL Agent jobs
                //
                foreach (Job job in j.Jobs)
                {
                    //
                    // Create the script
                    //
                    m_ScriptingOptions.FileName = Path.Combine(path, ConvertToFileName(job.Name.ToString()));
                    SendLogMessage(null, job.Name, m_ScriptingOptions.FileName, SQLServerScripterType.SQLAgentJob);
                    job.Script(m_ScriptingOptions);
                    files++;
                }
            }
            if (files == 0 && Directory.Exists(path))
                Directory.Delete(path, true);
        }

        /// <summary>
        /// Strips out invalid file/path characters from the DB Object Name
        /// </summary>
        /// <param name="DBObject">Database Object Name</param>
        /// <returns>Clean filename string</returns>
        private string ConvertToFileName(string DBObject)
        {
            string invalid = new string(Path.GetInvalidFileNameChars()) + new string(Path.GetInvalidPathChars());

            foreach (char c in invalid)
                DBObject = DBObject.Replace(c.ToString(), "");

            return DBObject + ".sql";
        }

        /// <summary>
        /// Delete all SQL Files in a folder
        /// </summary>
        /// <param name="path">Folder name</param>
        private void DeleteAllSQLFiles(string path)
        {
            DirectoryInfo folder = new DirectoryInfo(path);

            foreach (FileInfo file in folder.GetFiles("*.sql"))
                file.Delete();
        }
    }
}
