using System;

namespace MSSQLPostgreMigrator.Domains
{
    class Column
    {
        public Column() { }

        public string data { get; set; }
        public string from { get; set; }
        public string to { get; set; }
    }
}
