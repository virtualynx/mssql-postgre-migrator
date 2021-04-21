namespace MSSQLPostgreMigrator.Domains
{
  class Table
    {
        public Table(){

        }

        public string from{get;set;}
        public string to{get;set;}
        public Column[] columns{get;set;}
    }
}
