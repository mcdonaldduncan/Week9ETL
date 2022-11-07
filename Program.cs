namespace Week9ETL
{
    internal class Program
    {
        static void Main(string[] args)
        {
            List<Error> errors = new List<Error>();
            ETLProcess process = new ETLProcess();
            errors.AddRange(process.GenerateReport1());
            errors.AddRange(process.GenerateReport2());
            errors.AddRange(process.GenerateReport3());
            errors.AddRange(process.GenerateReport4());
        }
    }
}