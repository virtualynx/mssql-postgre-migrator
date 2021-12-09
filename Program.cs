using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using MSSQLPostgreMigrator.Domains;
using MSSQLPostgreMigrator.Services;

namespace MSSQLPostgreMigrator
{
    static class Program
    {
        private static void Main(string[] args)
        {
            Map map = null;
            using (StreamReader r = new("map.json"))
            {
                string json = r.ReadToEnd();
                map = JsonSerializer.Deserialize<Map>(json);
            }

            Mapper mapper = new(map.source, map.destination);

            mapper.OpenConnection();
            mapper.BeginTransaction();

            try
            {
                foreach (Table ltable in map.tables)
                {
                    mapper.map(ltable);
                }

                mapper.TransactionCommit();
            }
            catch (Exception)
            {
                mapper.TransactionRollback();
                throw;
            }
            finally
            {
                mapper.CloseConnection();
            }

            Console.WriteLine("Hello World!");
        }
    }
}
