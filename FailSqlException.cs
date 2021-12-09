using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using MSSQLPostgreMigrator.Domains;
using MSSQLPostgreMigrator.Services;

namespace MSSQLPostgreMigrator
{
    class FailSqlException : Exception
    {
        public FailSqlException(string message) : base(message)
        {
        }

        public FailSqlException() : base()
        {
        }

        public FailSqlException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
