using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace InfluxPush
{
    public class InfluxDataOptions
    {
        public string ApiToken { get; init; }
        public string Bucket { get; init; }

        public string Org { get; init; }

        public string Url { get; init; }

        public string MeasurementName { get; init; }

        public List<string> TagsList { get; init; }

        public string Delimiter { get; init; }

        public string DriveFormat { get; init; }

        public string DirectoryPath { get; init; }

    }
}
