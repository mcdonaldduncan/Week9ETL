using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Week9ETL
{
    internal class Constants
    {
        private const string folderName = "temp";

        public static string directoryPath => Path.Combine(Directory.GetCurrentDirectory(), folderName);

        public static string ReportTableName(int reportNumber)
        {
            return $"[dbo].[Report_{reportNumber} Table]";
        }

        public static string ReportFileName(int reportNumber)
        {
            return @$"Report{reportNumber}.txt";
        }

    }
}
