using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using MSSQLPostgreMigrator.Domains;
using Newtonsoft.Json;
using Npgsql;

namespace MSSQLPostgreMigrator.Services
{
    internal class Mapper
    {
        private readonly SqlConnection sqlConnection;
        private readonly NpgsqlConnection postgreConnection;
        private readonly string sourceSchema;
        private readonly string destinationSchema;
        private NpgsqlTransaction transaction;

        public Mapper(DbLoginInfo source, DbLoginInfo destination)
        {
            sourceSchema = source.schema;
            destinationSchema = destination.schema;
            sqlConnection = new SqlConnection("Data Source=" + source.host + "," + source.port + ";Initial Catalog=" + source.dbname + ";User ID=" + source.username + ";Password=" + source.password);
            postgreConnection = new NpgsqlConnection("Server=" + destination.host + ";Port=" + destination.port + ";User ID=" + destination.username + ";Password=" + destination.password + ";Database=" + destination.dbname + ";SearchPath=public");
        }

        public void OpenConnection()
        {
            sqlConnection.Open();
            postgreConnection.Open();
        }

        public void CloseConnection()
        {
            sqlConnection.Close();
            postgreConnection.Close();
        }

        public NpgsqlTransaction BeginTransaction()
        {
            transaction = postgreConnection.BeginTransaction();
            return transaction;
        }

        public void TransactionRollback()
        {
            transaction.Rollback();
        }

        public void TransactionCommit()
        {
            transaction.Commit();
        }

        public int map(Table table)
        {
            var sqlCommand = "";

            try
            {
                List<string> fieldFroms = new();
                List<string> fieldFromsSelect = new();
                List<string> fieldTos = new();
                Dictionary<string, FieldTypeMap> fieldTypeMaps = new();
                // Dictionary<string, string> blankFrom = new();

                foreach (Column col in table.columns)
                {
                    var from = col.from;
                    var aliasData = col.data;

                    if (aliasData != null)
                    {
                        fieldFromsSelect.Add(aliasData + " AS " + from);
                    }
                    else
                    {
                        fieldFromsSelect.Add(from);
                    }

                    fieldFroms.Add(from);
                    fieldTos.Add(col.to);

                    fieldTypeMaps[from] = new FieldTypeMap()
                    {
                        from = from,
                        to = col.to
                    };
                }

                sqlCommand = "SELECT " + string.Join(',', fieldFromsSelect) + " from " + sourceSchema + "." + table.from;
                SqlCommand command;
                SqlDataReader dataReader;
                try
                {
                    command = new(sqlCommand, sqlConnection);
                    dataReader = command.ExecuteReader();
                }
                catch (Exception e)
                {
                    throw new FailSqlException("Fail to execute: " + sqlCommand + ". Reason: " + e.Message);
                }

                //populate data type - from
                List<string> colNames = Enumerable.Range(0, dataReader.FieldCount).Select(dataReader.GetName).ToList();
                for (int a = 0; a < dataReader.FieldCount; a++)
                {
                    if (colNames[a].Length == 0) continue;
                    fieldTypeMaps[colNames[a]].typeFrom = dataReader.GetFieldType(a);
                }

                //get source data
                List<Dictionary<string, object>> data = new();
                while (dataReader.Read())
                {
                    Dictionary<string, object> rowData = new();
                    for (int a = 0; a < dataReader.FieldCount; a++)
                    {
                        var value = dataReader.GetValue(a);
                        value = value.GetType() == typeof(System.DBNull) ? null : value;
                        rowData[colNames[a]] = value;
                    }
                    data.Add(rowData);
                }

                dataReader.Close();
                command.Dispose();

                ///////////////////////////////////


                NpgsqlCommand pgsqlCommand = new(
                    "SELECT \"" + string.Join("\",\"", fieldTos) + "\" from " + destinationSchema + "." + table.to
                , postgreConnection);

                NpgsqlDataReader pgdataReader = pgsqlCommand.ExecuteReader();

                //populate data type - to
                colNames = Enumerable.Range(0, pgdataReader.FieldCount).Select(pgdataReader.GetName).ToList();
                for (int a = 0; a < pgdataReader.FieldCount; a++)
                {
                    FieldTypeMap ftm = GetTypeMapByToField(fieldTypeMaps, colNames[a]);
                    ftm.typeTo = pgdataReader.GetFieldType(a);
                }

                pgdataReader.Close();
                pgsqlCommand.Dispose();

                //perform necessary conversion
                foreach (KeyValuePair<string, FieldTypeMap> lfieldType in fieldTypeMaps)
                {
                    FieldTypeMap ftm = lfieldType.Value;

                    if (ftm.to == "is_disabled" && ftm.from?.Length == 0)
                    {
                        foreach (Dictionary<string, object> ldata in data)
                        {
                            ldata["is_disabled"] = Convert.ToBoolean(ldata[ftm.from]);
                        }
                    }


                    if (ftm.typeTo == typeof(bool))
                    {
                        foreach (Dictionary<string, object> ldata in data)
                        {
                            ldata[ftm.from] = Convert.ToBoolean(ldata[ftm.from]);
                        }
                    }

                    if (ftm.to.IsIn("created_by", "modified_by", "disabled_by"))
                    {
                        foreach (Dictionary<string, object> ldata in data)
                        {
                            ldata[ftm.from] = JsonConvert.SerializeObject(new { id = ldata[ftm.from] });
                        }
                    }
                }

                //perform insertion
                const int batchSize = 20;
                string sql = "INSERT INTO \"" + destinationSchema + "\"." + table.to + "(\"" + string.Join("\",\"", fieldTos) + "\") VALUES";
                List<string> sqlParams = new();
                for (int rowNum = 1; rowNum <= data.Count; rowNum++)
                {
                    Dictionary<string, object> ldata = data[rowNum - 1];
                    string p = "";
                    foreach (string lfield in fieldFroms)
                    {
                        string encloser_open = "";
                        string encloser_close = "";
                        if (
                          ldata[lfield] != null
                          && (
                            ldata[lfield].GetType() == typeof(string)
                            || ldata[lfield].GetType() == typeof(System.DateTime)
                          )
                        )
                        {
                            encloser_open = "'";
                            encloser_close = "'";
                        }
                        p += (p.Length > 0 ? "," : "") + encloser_open + (ldata[lfield] ?? "NULL") + encloser_close;
                    }
                    p = "(" + p + ")";
                    sqlParams.Add(p);
                    if (rowNum % batchSize == 0 || (rowNum == data.Count && sqlParams.Count > 0))
                    {
                        sqlCommand = sql + string.Join(',', sqlParams);
                        pgsqlCommand = new NpgsqlCommand(sqlCommand, postgreConnection)
                        {
                            Transaction = transaction
                        };
                        int affected = pgsqlCommand.ExecuteNonQuery();
                        sqlParams.Clear();
                    }
                }
            }
            catch (Exception e)
            {
                throw new FailSqlException("Fail to execute: " + sqlCommand + ". Reason: " + e.Message);
            }

            return 0;
        }

        private static FieldTypeMap GetTypeMapByToField(Dictionary<string, FieldTypeMap> fieldTypeMaps, string to)
        {
            foreach (KeyValuePair<string, FieldTypeMap> lfieldType in fieldTypeMaps)
            {
                if (lfieldType.Value.to.Equals(to))
                {
                    return lfieldType.Value;
                }
            }

            return null;
        }

        // https://stackoverflow.com/a/1344242/743891
        private static readonly Random random = new();
        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }

    public static class ObjectExtension
    {
        // https://stackoverflow.com/a/8192138/743891
        public static bool IsIn<T>(this T source, params T[] values)
        {
            return values.Contains(source);
        }
    }
}
