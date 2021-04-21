using System.Collections.Generic;
using System.IO;
using System;
using System.Text.Json;

using MSSQLPostgreMigrator.Domains;
using MSSQLPostgreMigrator.Services;

namespace MSSQLPostgreMigrator
{
    class Program
    {
        static void Main(string[] args)
        {
            Map map = null;
            using(StreamReader r = new StreamReader("map.json"))
            {
                string json = r.ReadToEnd();
                map = JsonSerializer.Deserialize<Map>(json);
            }

            Mapper mapper = new Mapper(map.source, map.destination);

            foreach(Table ltable in map.tables){
                mapper.map(ltable);
            }

            Console.WriteLine("Hello World!");
        }
    }
}
