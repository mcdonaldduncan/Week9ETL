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

        List<Error> errors = new List<Error>();

        public ETLProcess()
        {
            SqlConnectionStringBuilder sqlConStringBuilder = new SqlConnectionStringBuilder();
            sqlConStringBuilder["server"] = @"(localdb)\MSSQLLocalDB";
            sqlConStringBuilder["Trusted_Connection"] = true;
            sqlConStringBuilder["Integrated Security"] = "SSPI";
            sqlConStringBuilder["Initial Catalog"] = "PROG260FA22";

            SqlConString = sqlConStringBuilder.ToString();
        }

        public void RunETLProcess()
        {
            errors.AddRange(GenerateReport(1, @"ID|Full_Name|SSN|Full_Address|Phone"));
            errors.AddRange(GenerateReport(2, @"ID|Full_Name|Total_Courses|Courses_Complete|Courses_Incomplete|Courses_InProgress"));
            errors.AddRange(GenerateReport(3, @"Course_Code|Enrolled|Completed|Fail/Drop"));
            errors.AddRange(GenerateReport(4, @"Course_Code|Student_IDs|Primary_State"));

            foreach (var error in errors)
            {
                Console.WriteLine($"Error: {error.ErrorMessage} Source: {error.Source}");
            }
        }

        public List<Error> GenerateReport(int reportNumber, string columnNames)
        {
            Dictionary<int, List<string>> lines = new Dictionary<int, List<string>>();
            int fields = 0;
            try
            {
                using (SqlConnection conn = new SqlConnection(SqlConString))
                {
                    conn.Open();

                    string spName = $@"[dbo].[sp_GenerateReport{reportNumber}]";

                    using (var command = new SqlCommand(spName, conn))
                    {
                        command.CommandType = CommandType.StoredProcedure;

                        var reader = command.ExecuteReader();
                        int index = 0;

                        while (reader.Read())
                        {
                            fields = reader.FieldCount;
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

                errors.AddRange(ExportData(ReportFileName(reportNumber), columnNames, lines, out MyFile reportFile));
                errors.AddRange(ImportReportData(reportFile, reportNumber));
            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source + reportNumber));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source + reportNumber));
            }


            return errors;
        }

        private List<Error> ImportReportData(MyFile file, int reportNumber)
        {
            List<string[]> lines = new List<string[]>();

            int nonDataLineCount = 2;

            try
            {
                using (StreamReader sr = new StreamReader(file.FilePath))
                {
                    int index = 0;
                    while (!sr.EndOfStream)
                    {
                        var lineItems = sr.ReadLine()?.Split(file.Delimiter) ?? new string[0];

                        if (index > nonDataLineCount)
                        {
                            lines.Add(lineItems);
                        }
                        
                        index++;
                    }
                }

                using (SqlConnection conn = new SqlConnection(SqlConString))
                {
                    conn.Open();

                    string sproc = @$"[dbo].[sp_InsertReport{reportNumber}Data]";


                    foreach (var item in lines)
                    {
                        using (var cmd = new SqlCommand(sproc, conn))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            
                            switch (reportNumber)
                            {
                                case 1:

                                    cmd.Parameters.AddWithValue("@StudentID", item[0]);
                                    cmd.Parameters.AddWithValue("@FullName", item[1]);
                                    cmd.Parameters.AddWithValue("@SSN", item[2]);
                                    cmd.Parameters.AddWithValue("@FullAddress", item[3]);
                                    cmd.Parameters.AddWithValue("@Phone", item[4]);

                                    break;
                                case 2:

                                    cmd.Parameters.AddWithValue("@StudentID", item[0]);
                                    cmd.Parameters.AddWithValue("@FullName", item[1]);
                                    cmd.Parameters.AddWithValue("@CourseCount", item[2]);
                                    cmd.Parameters.AddWithValue("@Complete", item[3]);
                                    cmd.Parameters.AddWithValue("@Incomplete", item[4]);
                                    cmd.Parameters.AddWithValue("@InProgress", item[5]);

                                    break;
                                case 3:

                                    cmd.Parameters.AddWithValue("@Code", item[0]);
                                    cmd.Parameters.AddWithValue("@Enrolled", item[1]);
                                    cmd.Parameters.AddWithValue("@Completed", item[2]);
                                    cmd.Parameters.AddWithValue("@Failed", item[3]);

                                    break;
                                case 4:

                                    cmd.Parameters.AddWithValue("@Code", item[0]);
                                    cmd.Parameters.AddWithValue("@IDs", item[1]);
                                    cmd.Parameters.AddWithValue("@PrimaryState", item[2]);

                                    break;
                                default:
                                    errors.Add(new Error("Error modifying cmd", $"Report number {reportNumber}"));
                                    break;
                            }
                            
                            cmd.ExecuteNonQuery();
                        }
                    }

                    conn.Close();
                }

            }
            catch (IOException ioe)
            {
                errors.Add(new Error(ioe.Message, ioe.Source + reportNumber));
            }
            catch (Exception e)
            {
                errors.Add(new Error(e.Message, e.Source + reportNumber));
            }
            return errors;
        }

        private List<Error> ExportData(string newFileName, string includedColumns, Dictionary<int, List<string>> data, out MyFile newFile)
        {
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
