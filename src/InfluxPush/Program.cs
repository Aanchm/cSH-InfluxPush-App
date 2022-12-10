using System;
using Serilog;
using System.Collections.Generic;
using InfluxPush;
using Serilog.Exceptions;
using System.CommandLine;

namespace InfluxPush
{
    class Program
    {
        static void Main(string[] args)
        {

            var rc = BuildRootCommand();
            rc.Invoke(args);

        }

        static RootCommand BuildRootCommand()
        {
            #region Args specification

            var influxToken = new Option<string>(
                                                 new[] { "--influxToken", "-it" },
                                                 "Token for writing to influx")
            { IsRequired = true };

            var bucket = new Option<string>(
                                            new[] { "--bucket", "-b" },
                                            "Bucket to write to")
            { IsRequired = true };

            var org = new Option<string>(
                                         new[] { "--org", "-o" },
                                         "Organisation space in InfluxDB to write to")
            { IsRequired = true };

            var url = new Option<string>(
                                         new[] { "--url", "-u" },
                                         @"URL of the influx DB (i.e. 'http:\\192.168.10.10:8086'")
            {
                IsRequired = true
            };

            var measurementName = new Option<string>(
                             new[] { "--measurementName", "-mn" },
                             @"Name of the measurement in InfluxDb")
            {
                IsRequired = true
            };

            var delimiter = new Option<string>(
                 new[] { "--delimiter", "-d" },
                 @"Delimiter in the data")
            {
                IsRequired = true
            };

            var driveFormat = new Option<string>(
                new[] { "--driveFormat", "-dF" },
                @"Format of the drive Data (either X-XX or XXX-DRV-XXX)")
            {
                IsRequired = true
            };

            var directoryPath = new Option<string>(
                new[] { "--directoryPath", "-dP" },
                @"Path to the Directory")
            {
                IsRequired = true
            };


            #endregion

            var rc = new RootCommand("Get data")
            {
               influxToken,
               bucket,
               org,
               url,
               measurementName,
               delimiter,
               directoryPath
            };

            rc.SetHandler((string it, string b, string o, string u, string mn, string d, string dP) =>
            {

                AppLogic(it, b, o, u, mn, d, dP);
            }, influxToken, bucket, org, url, measurementName, delimiter, directoryPath);

            return rc;
        }

        static void AppLogic(string influxToken, string bucket, string org, string url, string measurementName, string delimiter, string directoryPath)
        {
            var log = new LoggerConfiguration().WriteTo
                .Console()
                .Enrich.WithExceptionDetails()
                .WriteTo.File("log.txt", rollingInterval: RollingInterval.Hour, retainedFileCountLimit: 10)
                .CreateLogger();

            log.Information("influx-token:{@influxToken}, bucket:{@bucket}, org:{@org}, url:{@url}, measurementName:{@measurementName}, delimiter:{@delimiter}, directoryPath:{@directoryPath}", influxToken, bucket, org, url, measurementName, delimiter, directoryPath);

            List<string> tagsList = new List<string> { "Set_ID", "Date",};

            var dataOpts = new InfluxDataOptions
            {
                Bucket = bucket,
                ApiToken = influxToken,
                Url = url,
                Org = org,
                MeasurementName = measurementName,
                TagsList = tagsList,
                Delimiter = delimiter,
                DirectoryPath = directoryPath
            };

            var files = new ReadData(dataOpts, log);
        }
    }
}
