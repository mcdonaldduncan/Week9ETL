using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Week9ETL
{
    internal class MyFile
    {
        public string Delimiter { get; set; }
        public string FilePath { get; set; }
        public string Extension { get; set; }

        public MyFile()
        {
        }

        public MyFile(string _delimiter, string _filePath, string extension)
        {
            Delimiter = _delimiter;
            FilePath = _filePath;
            Extension = extension;
        }
    }
}
