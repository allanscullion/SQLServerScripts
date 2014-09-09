# SQLServerScripts

Uses Microsoft.SqlServer.Management.Smo to create scripts for all objects in SQL Server. Useful for tracking database changes in GIT or SVN.

## Overview

*   [Requirements](#requirements)
*   [Description](#description)
*   [Output](#output)
*   [Interface](#interface)

## Requirements

This project uses assemblies from the Microsoft.SqlServer.Management.Smo namespace. You must have SQL Server Management Studio installed.

## Description

Will create scripts for every (useful) object on SQL Server (tested on SQL2008R2).

This includes:

*   Logins
*   SQLAgent Jobs
*   For each (non-system Database):
    *   CREATE DATABASE scripts
    *   Users
    *   Schemas
    *   Database Roles
    *   Application Roles
    *   Tables - including:
        *   Indexes
        *   Triggers
        *   Grant/Revoke statements
    *   Views
    *   Stored Procedures
    *   Functions
    *   Synonyms
    *   User-Defined Types
    *   User-Defined Data Types
    *   User-Defined Table Types

## Output

From the working folder, it will create the following folder structure containing individual scripts for each SQL Server Object:

*   {SERVERNAME}
    *   Databases
        *   {DBNAME1} - Root folder contains the CREATE DATABASE Script
            *   Functions
            *   Procs
            *   Roles - Application
            *   Roles - Database
            *   Schemas
            *   Synonyms
            *   Tables
            *   Types
                *   User-Defined Types
                *   User-Defined Data Types
                *   User-Defined Table Types
            *   Users
            *   Views
        *   {DBNAME2}...
            *   ...
    *   Logins
    *   SQLAgent

Things to note:

*   System databases are ignored
*   System owned objects are ignored
*   Bad file name characters are stripped from object names before creating the files
*   Login scripts have the random password removed and changed to '**CHANGEME**' (avoids unnecessary source control commits)
*   **IMPORTANT** - To help track deleted database objects, all "*.sql" files are deleted from the target folder before creating the new version

## Interface

Example usage:

```c#
    SQLServerScripter srv = new SQLServerScripter();
    srv.LogMessage += LogMessage; // Event handler for LogMessages

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
```

Example LogMessage Event Handler:

```c#
    static void LogMessage(object sender, SQLServerScripterMessageArgs m)
    {
        Console.WriteLine("----");
        Console.WriteLine(String.Format("Server: {0}", m.Server));
        if (m.Database != null)
            Console.WriteLine(String.Format("Database: {0}", m.Database));

        Console.WriteLine(String.Format("Object: {0}.{1}", m.ObjectType, m.ObjectName));
        Console.WriteLine(String.Format("Output File: {0}", m.Path));
    }
```
