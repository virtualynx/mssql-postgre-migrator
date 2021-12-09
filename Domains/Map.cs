namespace MSSQLPostgreMigrator.Domains
{
    class Map
    {
        public Map()
        {

        }

        public DbLoginInfo source { get; set; }
        public DbLoginInfo destination { get; set; }
        public Table[] tables { get; set; }
    }
}
