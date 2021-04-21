using System.Dynamic;
using System.Collections.Generic;
using System.Linq;
using System;
using System.Text.Json;
using MSSQLPostgreMigrator.Domains;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace MSSQLPostgreMigrator.Services
{
  class Mapper
    {
        private SqlConnection sqlConnection;
        private NpgsqlConnection postgreConnection;
        private string sourceSchema;
        private string destinationSchema;

        public Mapper(DbLoginInfo source, DbLoginInfo destination){
          sourceSchema = source.schema;
          destinationSchema = destination.schema;
          sqlConnection = new SqlConnection("Data Source="+source.host+","+source.port+";Initial Catalog="+source.dbname+";User ID="+source.username+";Password="+source.password);
          postgreConnection = new NpgsqlConnection("Server="+destination.host+";Port="+destination.port+";User ID="+destination.username+";Password="+destination.password+";Database="+destination.dbname+";SearchPath=public");
            
        }

        public int map(Table table){
          try{
            List<string> fieldFroms = new List<string>();
            List<string> fieldTos = new List<string>();
            Dictionary<string, FieldTypeMap> fieldTypeMaps = new Dictionary<string, FieldTypeMap>();
            
            foreach(Column col in table.columns){
              fieldFroms.Add(col.from);
              fieldTos.Add(col.to);
              fieldTypeMaps[col.from] = new FieldTypeMap(){
                from = col.from,
                to = col.to
              };
            }

            sqlConnection.Open();
            SqlCommand command = new SqlCommand(
              "SELECT "+String.Join(',',fieldFroms)+" from "+sourceSchema+"."+table.from
            , sqlConnection);
            SqlDataReader dataReader = command.ExecuteReader();

            //populate data type - from
            List<string> colNames = Enumerable.Range(0, dataReader.FieldCount).Select(dataReader.GetName).ToList();
            for(int a=0;a<dataReader.FieldCount;a++){
              fieldTypeMaps[colNames[a]].typeFrom = dataReader.GetFieldType(a);
            }

            //get source data
            List<Dictionary<string, object>> data = new List<Dictionary<string, object>>();
            while(dataReader.Read()){
              Dictionary<string, object> rowData = new Dictionary<string, object>();
              for(int a=0;a<dataReader.FieldCount;a++){
                var value = dataReader.GetValue(a);
                value = value.GetType()==typeof(System.DBNull)?null:value;
                rowData[colNames[a]] = value;
              }
              data.Add(rowData);
            }

            dataReader.Close();
            command.Dispose();

            ///////////////////////////////////

            postgreConnection.Open();
            NpgsqlCommand pgsqlCommand = new NpgsqlCommand(
              "SELECT \""+String.Join("\",\"",fieldTos)+"\" from "+destinationSchema+"."+table.to
            , postgreConnection);

            NpgsqlDataReader pgdataReader = pgsqlCommand.ExecuteReader();

            //populate data type - to
            colNames = Enumerable.Range(0, pgdataReader.FieldCount).Select(pgdataReader.GetName).ToList();
            for(int a=0;a<pgdataReader.FieldCount;a++){
              FieldTypeMap ftm = this.getTypeMapByToField(fieldTypeMaps, colNames[a]);
              ftm.typeTo = pgdataReader.GetFieldType(a);
            }

            pgdataReader.Close();
            pgsqlCommand.Dispose();

            //perform necessary conversion
            foreach(KeyValuePair<string, FieldTypeMap> lfieldType in fieldTypeMaps){
              FieldTypeMap ftm = lfieldType.Value;
              if(ftm.typeTo == typeof(System.Boolean)){
                foreach(Dictionary<string, object> ldata in data){
                  ldata[ftm.from] = Convert.ToBoolean(ldata[ftm.from]);
                }
              }
            }

            //perform insertion
            int batchSize = 20;
            string sql = "INSERT INTO \""+destinationSchema+"\"."+table.to+"(\""+String.Join("\",\"",fieldTos)+"\") VALUES";
            List<string> sqlParams = new List<string>();
            for(int rowNum=1;rowNum<=data.Count;rowNum++){
              Dictionary<string, object> ldata = data[rowNum-1];
              string p = "";
              foreach(string lfield in fieldFroms){
                string encloser_open = "";
                string encloser_close = "";
                if(
                  ldata[lfield] != null
                  &&(
                    ldata[lfield].GetType() == typeof(System.String)
                    ||ldata[lfield].GetType() == typeof(System.DateTime)
                  )
                ){
                  encloser_open = "'";
                  encloser_close = "'";
                }
                p += (p.Length>0?",":"")+encloser_open+(ldata[lfield]==null?"NULL":ldata[lfield])+encloser_close;
              }
              p = "("+p+")";
              sqlParams.Add(p);
              if(rowNum % batchSize==0 || (rowNum==data.Count && sqlParams.Count>0)){
                pgsqlCommand = new NpgsqlCommand(sql+String.Join(',',sqlParams), postgreConnection);
                int affected = pgsqlCommand.ExecuteNonQuery();
                sqlParams.Clear();
              }
            }
          }catch(Exception ex){
            throw;
          }finally{
            sqlConnection.Close();
            postgreConnection.Close();
          }

          return 0;
        }

        private FieldTypeMap getTypeMapByToField(
          Dictionary<string, FieldTypeMap> fieldTypeMaps,
          string to
        ){
          foreach(KeyValuePair<string, FieldTypeMap> lfieldType in fieldTypeMaps){
            if(lfieldType.Value.to.Equals(to)){
              return lfieldType.Value;
            }
          }

          return null;
        }
    }
}
