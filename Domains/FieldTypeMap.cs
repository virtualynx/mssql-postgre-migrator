using System;

namespace MSSQLPostgreMigrator.Domains
{
    class FieldTypeMap
    {
        public FieldTypeMap(){}

        public string from{get;set;}
        public string to{get;set;}
        public Type typeFrom{get;set;}
        public Type typeTo{get;set;}
    }
}
