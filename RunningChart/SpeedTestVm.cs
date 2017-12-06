using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LiveCharts.Geared;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Newtonsoft.Json;

namespace Geared.Wpf.SpeedTest
{
    public class SpeedTestVm : INotifyPropertyChanged
    {
        private double _trend;
        private double _count;
        private double _currentLecture;
        private bool _isHot;
        private HashSet<string> _bucketKeys;
        static string _bucketName = "lese-temperature-pressure";
        static IAmazonS3 client;
        Queue<TPCorr> _dataQueue;
        static object _lockObject;

        public SpeedTestVm()
        {
            Values = new GearedValues<double>().WithQuality(Quality.Highest);
            ReadCommand = new RelayCommand(Read);
            StopCommand = new RelayCommand(Stop);
            CleaCommand = new RelayCommand(Clear);

            _bucketKeys = new HashSet<string>();
            _dataQueue = new Queue<TPCorr>();
            _lockObject = new object();
        }

        public bool IsReading { get; set; }
        public RelayCommand ReadCommand { get; set; }
        public RelayCommand StopCommand { get; set; }
        public RelayCommand CleaCommand { get; set; }
        public GearedValues<double> Values { get; set; }

        public double Count
        {
            get { return _count; }
            set
            {
                _count = value;
                OnPropertyChanged("Count");
            }
        }

        public double CurrentLecture
        {
            get { return _currentLecture; }
            set
            {
                _currentLecture = value;
                OnPropertyChanged("CurrentLecture");
            }
        }

        public bool IsHot
        {
            get { return _isHot; }
            set
            {
                var changed = value != _isHot;
                _isHot = value;
                if (changed) OnPropertyChanged("IsHot");
            }
        }

        private void Stop()
        {
            IsReading = false;
        }

        private void Clear()
        {
            Values.Clear();
        }

        private void Read()
        {
            if (IsReading) return;

            //lets keep in memory only the last 20000 records,
            //to keep everything running faster
            const int keepRecords = 20000;
            IsReading = true;

            Action updateData = () =>
            {
                while (IsReading)
                {
                    Thread.Sleep(1);
                    UpdateData();
                }
            };

            Action readFromTread = () =>
            {
                while (IsReading)
                {
                    bool hasNewData = false;
                    Thread.Sleep(1);
                    var r = new Random();
                    //_trend += (r.NextDouble() < 0.5 ? 1 : -1) * r.Next(0, 10) * .001;
                    lock (_lockObject)
                    {
                        if (_dataQueue.Count > 0)
                        {
                            TPCorr tpCorr = _dataQueue.Dequeue();
                            if (tpCorr.correlation_coefficient.HasValue)
                            {
                                hasNewData = true;
                                _trend = tpCorr.correlation_coefficient.Value;
                            }
                        }
                    }

                    //when multi threading avoid indexed calls like -> Values[0] 
                    //instead enumerate the collection
                    //ChartValues/GearedValues returns a thread safe copy once you enumerate it.
                    //TIPS: use foreach instead of for
                    //LINQ methods also enumerate the collections
                    if (hasNewData)
                    {
                        var first = Values.DefaultIfEmpty(0).FirstOrDefault();
                        if (Values.Count > keepRecords - 1) Values.Remove(first);
                        if (Values.Count < keepRecords) Values.Add(_trend);
                        IsHot = _trend > 0;
                        Count = Values.Count;
                        CurrentLecture = _trend;
                    }
                }
            };

            //2 different tasks adding a value every ms
            //add as many tasks as you want to test this feature
            Task.Factory.StartNew(readFromTread);
            Task.Factory.StartNew(readFromTread);
            Task.Factory.StartNew(readFromTread);
            //Task.Factory.StartNew(readFromTread);
            //Task.Factory.StartNew(readFromTread);
            //Task.Factory.StartNew(readFromTread);
            //Task.Factory.StartNew(readFromTread);
            //Task.Factory.StartNew(readFromTread);

            Task.Factory.StartNew(updateData);
        }

        public event PropertyChangedEventHandler PropertyChanged;

        protected virtual void OnPropertyChanged(string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        private void UpdateData()
        {
            Amazon.Runtime.CredentialManagement.SharedCredentialsFile credFile = new Amazon.Runtime.CredentialManagement.SharedCredentialsFile(@"C:\Users\mvlese\.aws\credentials");
            Amazon.Runtime.CredentialManagement.CredentialProfile prof;
            credFile.TryGetProfile("default", out prof);
            var cred = prof.GetAWSCredentials(credFile);

            using (IAmazonKinesis klient = new AmazonKinesisClient(cred, Amazon.RegionEndpoint.USWest2))
            {
                ListStreamsResponse resp = klient.ListStreams();
                DescribeStreamRequest describeRequest = new DescribeStreamRequest();
                string kinesisStreamName = "lese_temperature_pressure_json";
                describeRequest.StreamName = kinesisStreamName;

                DescribeStreamResponse describeResponse = klient.DescribeStream(describeRequest);
                List<Shard> shards = describeResponse.StreamDescription.Shards;

                foreach (Shard shard in shards)
                {
                    GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest();
                    iteratorRequest.StreamName = kinesisStreamName;
                    iteratorRequest.ShardId = shard.ShardId;
                    iteratorRequest.ShardIteratorType = ShardIteratorType.TRIM_HORIZON;

                    GetShardIteratorResponse iteratorResponse = klient.GetShardIterator(iteratorRequest);
                    string iteratorId = iteratorResponse.ShardIterator;

                    while (!string.IsNullOrEmpty(iteratorId))
                    {
                        Thread.Sleep(1);
                        GetRecordsRequest getRequest = new GetRecordsRequest();
                        getRequest.Limit = 5000;
                        getRequest.ShardIterator = iteratorId;

                        GetRecordsResponse getResponse = klient.GetRecords(getRequest);
                        string nextIterator = getResponse.NextShardIterator;
                        List<Record> records = getResponse.Records;

                        if (records.Count > 0)
                        {
                            Console.WriteLine("Received {0} records. ", records.Count);
                            foreach (Record record in records)
                            {
                                Thread.Sleep(1);
                                string json = Encoding.UTF8.GetString(record.Data.ToArray());
                                //Console.WriteLine("Json string: " + json);
                                TPCorr tpCorr = JsonConvert.DeserializeObject<TPCorr>(json);
                                if (tpCorr.ts.HasValue)
                                {
                                    DateTime ts = tpCorr.ts.Value;
                                    Console.WriteLine(ts);
                                    if (ts > DateTime.Now.AddHours(-6))
                                    {
                                        lock (_lockObject)
                                        {
                                            _dataQueue.Enqueue(tpCorr);
                                        }
                                    }
                                }

                            }
                        }
                        iteratorId = nextIterator;

                        if (!IsReading)
                        {
                            break;
                        }
                    }

                    if (!IsReading)
                    {
                        break;
                    }

                }
            }

            #region Bucket Stuff
            //using (client = new AmazonS3Client(cred, Amazon.RegionEndpoint.USWest2))
            //{
            //    ListBucketsResponse lbr = client.ListBuckets();

            //    IList<string> objectKeys = client.GetAllObjectKeys(_bucketName, "2017", null);
            //    foreach (string keyName in objectKeys)
            //    {
            //        try
            //        {
            //            if (!_bucketKeys.Contains(keyName))
            //            {
            //                _bucketKeys.Add(keyName);

            //                GetObjectRequest request = new GetObjectRequest
            //                {
            //                    BucketName = _bucketName,
            //                    Key = keyName
            //                };

            //                using (GetObjectResponse response = client.GetObject(request))
            //                {
            //                    using (Stream responseStream = response.ResponseStream)
            //                    {
            //                        using (StreamReader reader = new StreamReader(responseStream))
            //                        {
            //                            responseBody = reader.ReadToEnd();
            //                            string[] lines = responseBody.Split(new char[] { '\n' });
            //                            foreach (string line in lines)
            //                            {
            //                                string[] data = line.Split(new char[] { ',' });
            //                                if (data.Length == 5)
            //                                {
            //                                    TP tp = new TP()
            //                                    {
            //                                        CorrelationCoefficient = GetFloat(data[2]),
            //                                        Temperature = GetFloat(data[3]),
            //                                        Pressure = GetFloat(data[4])
            //                                    };
            //                                    lock (_lockObject)
            //                                    {
            //                                        _dataQueue.Enqueue(tp);
            //                                    }
            //                                    Console.WriteLine("Adding to queue...");
            //                                }
            //                                Thread.Sleep(1);
            //                            }
            //                        }
            //                    }
            //                }
            //            }
            //        }
            //        catch (Exception ex)
            //        {
            //            Console.WriteLine(ex.ToString());
            //        }
            //    }
            //}
            #endregion
        }

        private float GetFloat(string s)
        {
            float result = 0f;
            if (s != null)
            {
                float.TryParse(s, out result);
            }
            return result;
        }

        class TP
        {
            public float CorrelationCoefficient { get; set; }
            public float Temperature { get; set; }
            public float Pressure { get; set; }
        }
        class TPCorr
        {
            public DateTime? ts { get; set; }
            public string kind { get; set; }
            public float? correlation_coefficient { get; set; }
            public float? stdev_temperature { get; set; }
            public float? stdev_pressure { get; set; }
        }
    }
}
