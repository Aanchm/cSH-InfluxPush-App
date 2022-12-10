using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace InfluxPush
{
    internal class MyFileInfo
    {
        internal FileInfo MyFile { get; init; }

        internal string MyDate { get; init; }

        internal MyFileInfo(string filepath)
        {
            MyFile = new FileInfo(filepath);
            MyDate = MyFile.CreationTime.Date.ToString();
        }
    }
}
