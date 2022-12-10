using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using InfluxDB.Client;
using InfluxDB.Client.Writes;
using InfluxDB.Client.Api.Domain;
using System.Collections.Concurrent;
using Serilog.Core;
using Newtonsoft.Json;
using System.Diagnostics;

namespace InfluxPush
{
    public class ReadData : IDisposable
    {
        private string _url;
        private string _influxToken;
        private string _bucket;
        private string _organisation;
        private InfluxDBClient? _ifClient;
        private bool disposedValue;

        private string _filePath;
        private string _archivedFilePath;
        private string _errorFilePath;

        private string _measurement;
        private List<string> _tagsList = new List<string>();
        private string _delimiter;
        private string _driveFormat;
        int bufferMaxSize = 5000;

        Logger _log;
        Stopwatch _stopwatch = new Stopwatch();

        public ReadData(InfluxDataOptions dataOpts, Logger log)
        {
            _influxToken = dataOpts.ApiToken;
            _url = dataOpts.Url; 
            _organisation = dataOpts.Org;
            _bucket = dataOpts.Bucket;
            _measurement = dataOpts.MeasurementName;
            _tagsList = dataOpts.TagsList;
            _delimiter = dataOpts.Delimiter;
            _driveFormat = dataOpts.DriveFormat;
            _filePath = dataOpts.DirectoryPath;
            _log = log; 

            _archivedFilePath = $"{@_filePath}\\Archive\\";
            if (!Directory.Exists(_archivedFilePath))
                Directory.CreateDirectory(_archivedFilePath);

            _errorFilePath = $"{@_filePath}\\ErrorFiles\\";
            if (!Directory.Exists(_errorFilePath))
                Directory.CreateDirectory(_errorFilePath);

            try
            {
                ProcessFiles();
            }
            catch(AggregateException ae)
            {
                foreach (var ex in ae.Flatten().InnerExceptions)
                {
                    Console.WriteLine(ex.Message);
                    log.Error(ex, "New Exception Thrown");
                    Console.ReadKey();
                }
            }
        }

        public void Connect()
        {
           var influxOpts = new InfluxDBClientOptions.Builder().VerifySsl(false)
                                                                .Url(_url)
                                                                .Org(_organisation)
                                                                .AuthenticateToken(_influxToken)
                                                                .Build();

            _ifClient = InfluxDBClientFactory.Create(influxOpts);
            _log.Information("Client Connected");
        }

        private void ProcessFiles()
        {
            var exceptions = new ConcurrentQueue<Exception>();
            string[] filePaths = Directory.GetFiles(_filePath);

            var allFiles = filePaths.Select(item => new MyFileInfo(item))
                                    .ToList();

            var groupedFiles = from file in allFiles
                               group file by file.MyDate into groupedByDate
                               orderby groupedByDate.Key
                               select groupedByDate;


            var datasets = groupedFiles.Select((dateGroup, index) => new
            {
                Date = dateGroup.Key,
                SetID = $"{dateGroup.Key}_-{(index + 1).ToString().PadLeft(3, '0')}",
                Files = dateGroup.Where(item => item.MyFile.FullName.Contains(".csv") || item.MyFile.FullName.Contains(".json"))
            });

            var parallelOpts = new ParallelOptions
            {
                //Use 75% of Processors Available
                MaxDegreeOfParallelism = Convert.ToInt32(Math.Ceiling((Environment.ProcessorCount * 0.75) * 2.0))
            };

            Parallel.ForEach(datasets.ToList(), parallelOpts, dataset =>
            {
                try
                {
                    ProcessDataSet(dataset.Files.ToList(), dataset.SetID, dataset.Date);
                }
                catch (Exception ex)
                {
                    exceptions.Enqueue(ex);
                }
            });

            if (exceptions.Count > 0) throw new AggregateException(exceptions);
        }


        private void ProcessDataSet(List <MyFileInfo> files, string SetId, string date)
        {      
            var pointsBuffer = new List<PointData>();
            string defaultTimestamp = null;

            foreach (var file in files)
            {
                _stopwatch.Restart();
                _log.Information($"Writing {file.MyFile.Name}. Thread: {Thread.CurrentThread.ManagedThreadId}. FileSize: {file.MyFile.Length} bytes");
                
                if(file.MyFile.Length == 0)
                {
                    _log.Information($"{file.MyFile.Name} is empty");
                    string newFilePath = _archivedFilePath + file.MyFile.Name;
                    System.IO.File.Move(file.MyFile.FullName, newFilePath);
                    continue;
                }

                bool pushSuccessful = false;

                if (file.MyFile.Extension == ".csv")
                    pushSuccessful = SendCSVDataToInflux(file.MyFile.FullName, SetId, date, ref pointsBuffer, ref defaultTimestamp);

                if (file.MyFile.Extension == ".json")
                    pushSuccessful = SendJsonDataToInflux(file.MyFile.FullName, SetId, date, ref pointsBuffer, defaultTimestamp);
                
                if (pushSuccessful)
                {
                    _stopwatch.Stop();
                    string newFilePath = _archivedFilePath + file.MyFile.Name;
                    System.IO.File.Move(file.MyFile.FullName, newFilePath);
                    _log.Information($"Finished {file.MyFile.Name}. Upload Time: {_stopwatch.ElapsedMilliseconds} ms");
                }
                else
                {
                    string newFilePath = _errorFilePath + file.MyFile.Name;
                    System.IO.File.Move(file.MyFile.FullName, newFilePath);
                    _log.Information($"Could not send {file.MyFile.Name}");
                }
            }
        }

        private Dictionary<string, string> ReadJson(string Jsonpath)
        {
            using (var stream = new StreamReader(Jsonpath))
            {
                string jsonString = stream.ReadToEnd();
                var json = JsonConvert.DeserializeObject<Dictionary<string, string>>(jsonString);

                return json;
            }
        }


        private void WritePointsBufferToInflux(ref List<PointData> pointsBuffer)
        {
            if (pointsBuffer.Count == 0)
                return;

            if (_ifClient is null) 
                Connect();

            using (var _api = _ifClient?.GetWriteApi())
            {
                _api?.WritePoints(pointsBuffer, _bucket, _organisation);
            }
            pointsBuffer?.Clear();
        }

        private void WriteToPointsBuffer(Dictionary<string, string> record, ref List<PointData> pointsBuffer, string defaultTimestamp)
        {
            PointData? point = BuildPointWithMeasurementAndTimestamp(record, defaultTimestamp);
            point = AddTagsToPoint(point, record, _tagsList);
            Dictionary<string,string> strippedRecord = StripTagsAndTimestampFromRecord(record, _tagsList); 

            foreach (var (key, value) in strippedRecord)
            {                
                if (value == null)
                    continue;

                if (!record.TryGetValue(key, out var stringValue))
                {
                    _log.Information("Can not find record for field");
                    continue; 
                }
                             

                if (!double.TryParse(stringValue, out var doubleValue))
                    point = point?.Field(key, stringValue);

                else
                    point = point?.Field(key, doubleValue);
            }

            pointsBuffer?.Add(point);
        }

        private Dictionary<string, string> StripTagsAndTimestampFromRecord(Dictionary<string, string> record, List<string> tagsList)
        {
            foreach (var tag in tagsList)
            {
                record.Remove(tag);
            }
            record.Remove("Timestamp");
            return record; 
        }
        
        private PointData? AddTagsToPoint(PointData point, Dictionary<string, string> record, List<string> tagsList)
        {
            if (point == null)
            {
                _log.Information("point data does not exist");
                return null;  
            }

            foreach(var tag in tagsList)
            {
                if (record.TryGetValue(tag, out var tagValue))
                {
                    if (tagValue == null)
                        continue; 
                }
                point = point.Tag(tag, tagValue);
            }
            return point;      
        }


        private PointData BuildPointWithMeasurementAndTimestamp(Dictionary<string, string> record, string defaultTimestamp)
        {
            var point = PointData.Measurement(_measurement);

            if (!record.TryGetValue("Timestamp", out var timestamp))
                point = point.Timestamp((long)Convert.ToDouble(defaultTimestamp), WritePrecision.Ms);

            else
                point = point.Timestamp((long)Convert.ToDouble(timestamp), WritePrecision.Ms);

            return point; 
        }

        private bool SendCSVDataToInflux(string filename, string setID, string date, ref List<PointData> pointsBuffer, ref string defaultTimestamp)
        {
            int rowNumber = 0;

            CsvConfiguration csvConfiguration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                Delimiter = _delimiter,
                BadDataFound = null
            };

            bool successfullyParsed = true;
            int dodgyLineCount = 0; 

            using (CsvReader csv = new CsvReader(System.IO.File.OpenText(filename), csvConfiguration))
            {
                csv.Read();
                csv.ReadHeader();
                if (csv.HeaderRecord.Length == 1 || csv.HeaderRecord == null)
                {
                    _log.Error($"No headers. Shitty file cannot be parsed: {filename}");
                    successfullyParsed = false;
                }

                if (successfullyParsed)
                {

                    List<string> oldHeaders = csv.HeaderRecord.ToList();
                    List<string> headers = oldHeaders.Select(x => x.Replace(" ", "_")).ToList();

                    while (csv.Read())
                    {
                        rowNumber++;
                        string value;

                        List<string> data = new List<string>();

                        for (int i = 0; csv.TryGetField<string>(i, out value); i++)
                        {
                            if (value == "")
                                value = null;

                            data.Add(value);
                        }

                        if (!headers.Exists(x => x.Contains("Timestamp")))
                        {
                            _log.Error($"No Timestamp. Line ignored: {filename}");
                            successfullyParsed = false;
                            break;
                        }


                        if (dodgyLineCount >= 10)
                        {
                            _log.Error($"Too much shit data. File Aborted: {filename}");
                            successfullyParsed = false;
                            break;
                        }

                        if (headers.Count != data.Count)
                        {
                            _log.Error($"Shit data. Line ignored: {filename}");
                            dodgyLineCount++;
                            continue;
                        }


                        var csvDictionary = Enumerable.Range(0, headers.Count).ToDictionary(i => headers[i], i => data[i]);
                        csvDictionary.Add("Set_ID", setID);
                        csvDictionary.Add("Date", date);

                        if (rowNumber == 1)
                        {
                            if (csvDictionary.TryGetValue("Timestamp", out var firstTimestamp))
                                defaultTimestamp = firstTimestamp;
                            else
                            {
                                defaultTimestamp = DateTime.UnixEpoch.ToString();
                                throw new ArgumentException($"{filename}: No Timestamp Found");
                            }
                        }

                        WriteToPointsBuffer(csvDictionary, ref pointsBuffer, defaultTimestamp);

                        if (pointsBuffer.Count == bufferMaxSize)
                            WritePointsBufferToInflux(ref pointsBuffer);
                    }
                }
            }
            if (successfullyParsed)
                return true;
            else
                return false;
        }

        private bool SendJsonDataToInflux(string recipeFile, string setID, string date, ref List<PointData> pointsBuffer, string defaultTimestamp)
        {
            var recipe = ReadJson(recipeFile);
            recipe.Add("Run_ID", setID);
            recipe.Add("Date", date);
            WriteToPointsBuffer(recipe, ref pointsBuffer, defaultTimestamp);
            WritePointsBufferToInflux(ref pointsBuffer);
            return true; 
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _ifClient?.Dispose();
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}