using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using static Week9ETL.Constants;

namespace Week9ETL
{
    internal class ETLProcess
    {
        string SqlConString { get; set; }

        public ETLProcess()
        {
            SqlConnectionStringBuilder sqlConStringBuilder = new SqlConnectionStringBuilder();
            sqlConStringBuilder["server"] = @"(localdb)\MSSQLLocalDB";
            sqlConStringBuilder["Trusted_Connection"] = true;
            sqlConStringBuilder["Integrated Security"] = "SSPI";
            sqlConStringBuilder["Initial Catalog"] = "PROG260FA22";

            SqlConString = sqlConStringBuilder.ToString();
        }

        private string CreateHeader(string[] headerItems)
        {
            string header = "";

            for (int i = 0; i < headerItems.Length; i++)
            {
                if (i == 0)
                {
                    header += "(";
                }

                header += $@"[{headerItems[i]}]";

                if (i == headerItems.Length - 1)
                {
                    header += ")";
                }
                else
                {
                    header += ",";
                }
            }

            return header;
        }

        private List<Error> ImportDataReport1(MyFile file, int reportNumber)
        {
            List<Error> errors = new List<Error>();
            List<string[]> lines = new List<string[]>();
            string header;
            try
            {
                using (StreamReader sr = new StreamReader(file.FilePath))
                {
                    int index = 0;
                    while (!sr.EndOfStream)
                    {
                        if (index < 3)
                        {
                            var headerItems = sr.ReadLine()?.Split(file.Delimiter) ?? new string[0];
                            header = CreateHeader(headerItems);
                        }
                        else
                        {
                            var lineItems = sr.ReadLine()?.Split(file.Delimiter) ?? new string[0];
                            lines.Add(lineItems);
                        }
                        index++;
                    }
                }

                using (SqlConnection con = new SqlConnection(SqlConString))
                {
                    con.Open();

                    string sproc = @"[dbo].[sp_InsertReport1Data]";


                    foreach (var item in lines)
                    {
                        using (var cmd = new SqlCommand(sproc, con))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.Parameters.AddWithValue("@StudentID", item[0]);
                            cmd.Parameters.AddWithValue("@FullName", item[1]);
                            cmd.Parameters.AddWithValue("@SSN", item[2]);
                            cmd.Parameters.AddWithValue("@FullAddress", item[3]);
                            cmd.Parameters.AddWithValue("@Phone", item[4]);

                            cmd.ExecuteNonQuery();
                        }
                    }

                    con.Close();
                }

            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }
            return errors;
        }

        private List<Error> ImportDataReport2(MyFile file, int reportNumber)
        {
            List<Error> errors = new List<Error>();
            List<string[]> lines = new List<string[]>();
            string header;
            try
            {
                using (StreamReader sr = new StreamReader(file.FilePath))
                {
                    int index = 0;
                    while (!sr.EndOfStream)
                    {
                        if (index < 3)
                        {
                            var headerItems = sr.ReadLine()?.Split(file.Delimiter) ?? new string[0];
                            header = CreateHeader(headerItems);
                        }
                        else
                        {
                            var lineItems = sr.ReadLine()?.Split(file.Delimiter) ?? new string[0];
                            lines.Add(lineItems);
                        }
                        index++;
                    }
                }

                using (SqlConnection con = new SqlConnection(SqlConString))
                {
                    con.Open();

                    string sproc = @"[dbo].[sp_InsertReport2Data]";


                    foreach (var item in lines)
                    {
                        using (var cmd = new SqlCommand(sproc, con))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.Parameters.AddWithValue("@StudentID", item[0]);
                            cmd.Parameters.AddWithValue("@FullName", item[1]);
                            cmd.Parameters.AddWithValue("@CourseCount", item[2]);
                            cmd.Parameters.AddWithValue("@Complete", item[3]);
                            cmd.Parameters.AddWithValue("@Incomplete", item[4]);
                            cmd.Parameters.AddWithValue("@InProgress", item[5]);

                            cmd.ExecuteNonQuery();
                        }
                    }

                    con.Close();
                }

            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }
            return errors;
        }

        private List<Error> ImportDataReport3(MyFile file, int reportNumber)
        {
            List<Error> errors = new List<Error>();
            List<string[]> lines = new List<string[]>();
            string header;
            try
            {
                using (StreamReader sr = new StreamReader(file.FilePath))
                {
                    int index = 0;
                    while (!sr.EndOfStream)
                    {
                        if (index < 3)
                        {
                            var headerItems = sr.ReadLine()?.Split(file.Delimiter) ?? new string[0];
                            header = CreateHeader(headerItems);
                        }
                        else
                        {
                            var lineItems = sr.ReadLine()?.Split(file.Delimiter) ?? new string[0];
                            lines.Add(lineItems);
                        }
                        index++;
                    }
                }

                using (SqlConnection con = new SqlConnection(SqlConString))
                {
                    con.Open();

                    string sproc = @"[dbo].[sp_InsertReport3Data]";


                    foreach (var item in lines)
                    {
                        using (var cmd = new SqlCommand(sproc, con))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.Parameters.AddWithValue("@Code", item[0]);
                            cmd.Parameters.AddWithValue("@Enrolled", item[1]);
                            cmd.Parameters.AddWithValue("@Completed", item[2]);
                            cmd.Parameters.AddWithValue("@Failed", item[3]);

                            cmd.ExecuteNonQuery();
                        }
                    }

                    con.Close();
                }

            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }
            return errors;
        }

        private List<Error> ImportDataReport4(MyFile file, int reportNumber)
        {
            List<Error> errors = new List<Error>();
            List<string[]> lines = new List<string[]>();
            string header;
            try
            {
                using (StreamReader sr = new StreamReader(file.FilePath))
                {
                    int index = 0;
                    while (!sr.EndOfStream)
                    {
                        if (index < 3)
                        {
                            var headerItems = sr.ReadLine()?.Split(file.Delimiter) ?? new string[0];
                            header = CreateHeader(headerItems);
                        }
                        else
                        {
                            var lineItems = sr.ReadLine()?.Split(file.Delimiter) ?? new string[0];
                            lines.Add(lineItems);
                        }
                        index++;
                    }
                }

                using (SqlConnection con = new SqlConnection(SqlConString))
                {
                    con.Open();

                    string sproc = @"[dbo].[sp_InsertReport4Data]";


                    foreach (var item in lines)
                    {
                        using (var cmd = new SqlCommand(sproc, con))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.Parameters.AddWithValue("@Code", item[0]);
                            cmd.Parameters.AddWithValue("@IDs", item[1]);
                            cmd.Parameters.AddWithValue("@PrimaryState", item[2]);

                            cmd.ExecuteNonQuery();
                        }
                    }

                    con.Close();
                }

            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }
            return errors;
        }

        private List<Error> ExportData(string newFileName, string includedColumns, Dictionary<int, List<string>> data, out MyFile newFile)
        {
            List<Error> errors = new List<Error>();
            string writePath = Path.Combine(directoryPath, newFileName);

            try
            {
                if (File.Exists(writePath))
                {
                    File.Delete(writePath);
                }

                using (StreamWriter sw = new StreamWriter(writePath, true))
                {
                    sw.WriteLine($"Processed at: {DateTime.Now}");
                    sw.WriteLine();
                    sw.WriteLine(includedColumns);

                    foreach (var item in data)
                    {
                        string temp = "";
                        for (int i = 0; i < item.Value.Count; i++)
                        {
                            if (i == item.Value.Count - 1)
                            {
                                temp += item.Value[i];
                            }
                            else
                            {
                                temp += $"{item.Value[i]}|";
                            }
                        }
                        sw.WriteLine(temp);
                    }
                }
            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }

            newFile = new MyFile("|", writePath, @".txt");


            return errors;
        }

        public List<Error> GenerateReport1()
        {
            List<Error> errors = new List<Error>();
            Dictionary<int, List<string>> lines = new Dictionary<int, List<string>>();
            int fields = 5;

            try
            {
                using (SqlConnection conn = new SqlConnection(SqlConString))
                {
                    conn.Open();

                    string spName = $@"[dbo].[sp_GenerateReport1]";

                    using (var command = new SqlCommand(spName, conn))
                    {
                        command.CommandType = CommandType.StoredProcedure;

                        var reader = command.ExecuteReader();
                        int index = 0;
                        while (reader.Read())
                        {
                            List<string> temp = new List<string>();
                            for (int i = 0; i < fields; i++)
                            {
                                temp.Add(ConvertEmptyValue($"{reader.GetValue(i)}"));
                            }
                            lines.Add(index++, temp);
                        }

                        reader.Close();

                    }

                    conn.Close();
                }

                string columNames = "ID|Full_Name|SSN|Full_Address|Phone";
                errors.AddRange(ExportData(ReportFileName(1), columNames, lines, out MyFile reportFile));
                errors.AddRange(ImportDataReport1(reportFile, 1));

            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }


            return errors;
        }

        public List<Error> GenerateReport2()
        {
            List<Error> errors = new List<Error>();
            Dictionary<int, List<string>> lines = new Dictionary<int, List<string>>();
            int fields = 6;

            try
            {
                using (SqlConnection conn = new SqlConnection(SqlConString))
                {
                    conn.Open();

                    string spName = $@"[dbo].[sp_GenerateReport2]";

                    using (var command = new SqlCommand(spName, conn))
                    {
                        command.CommandType = CommandType.StoredProcedure;

                        var reader = command.ExecuteReader();
                        int index = 0;
                        while (reader.Read())
                        {
                            List<string> temp = new List<string>();
                            for (int i = 0; i < fields; i++)
                            {
                                temp.Add(ConvertEmptyValue($"{reader.GetValue(i)}"));
                            }
                            lines.Add(index++, temp);
                        }

                        reader.Close();

                    }

                    conn.Close();
                }

                string columNames = "ID|Full_Name|Total_Courses|Courses_Complete|Courses_Incomplete|Courses_InProgress";
                errors.AddRange(ExportData(ReportFileName(2), columNames, lines, out MyFile reportFile));
                errors.AddRange(ImportDataReport2(reportFile, 2));

            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }


            return errors;
        }

        public List<Error> GenerateReport3()
        {
            List<Error> errors = new List<Error>();
            Dictionary<int, List<string>> lines = new Dictionary<int, List<string>>();
            int fields = 4;

            try
            {
                using (SqlConnection conn = new SqlConnection(SqlConString))
                {
                    conn.Open();

                    string spName = $@"[dbo].[sp_GenerateReport3]";

                    using (var command = new SqlCommand(spName, conn))
                    {
                        command.CommandType = CommandType.StoredProcedure;

                        var reader = command.ExecuteReader();
                        int index = 0;
                        while (reader.Read())
                        {
                            List<string> temp = new List<string>();
                            for (int i = 0; i < fields; i++)
                            {
                                temp.Add(ConvertEmptyValue($"{reader.GetValue(i)}"));
                            }
                            lines.Add(index++, temp);
                        }

                        reader.Close();

                    }

                    conn.Close();
                }

                string columNames = @"Course_Code|Enrolled|Completed|Fail/Drop";
                errors.AddRange(ExportData(ReportFileName(3), columNames, lines, out MyFile reportFile));
                errors.AddRange(ImportDataReport3(reportFile, 3));

            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }


            return errors;
        }

        public List<Error> GenerateReport4()
        {
            List<Error> errors = new List<Error>();
            Dictionary<int, List<string>> lines = new Dictionary<int, List<string>>();
            int fields = 3;

            try
            {
                using (SqlConnection conn = new SqlConnection(SqlConString))
                {
                    conn.Open();

                    string spName = $@"[dbo].[sp_GenerateReport4]";

                    using (var command = new SqlCommand(spName, conn))
                    {
                        command.CommandType = CommandType.StoredProcedure;

                        var reader = command.ExecuteReader();
                        int index = 0;
                        while (reader.Read())
                        {
                            List<string> temp = new List<string>();
                            for (int i = 0; i < fields; i++)
                            {
                                temp.Add(ConvertEmptyValue($"{reader.GetValue(i)}"));
                            }
                            lines.Add(index++, temp);
                        }

                        reader.Close();

                    }

                    conn.Close();
                }

                string columNames = @"Course_Code|Student_IDs|Primary_State";
                errors.AddRange(ExportData(ReportFileName(4), columNames, lines, out MyFile reportFile));
                errors.AddRange(ImportDataReport4(reportFile, 4));

            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source));
            }


            return errors;
        }
        string ConvertEmptyValue(string init)
        {
            if (init == null || init == string.Empty)
            {
                return "null";
            }
            return init;
        }
    }
}
