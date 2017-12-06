using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Newtonsoft.Json;
using Amazon.Runtime;

namespace AwsLese
{
    public class StreamConsumer<T> : IDebugProducer
    {
        public delegate void DataHandler(T obj);

        private AWSCredentials _credentials;
        private DataHandler _dataHandler = null;

        public StreamConsumer(string streamName, AWSCredentials credentials, DataHandler dataHandler)
        {
            StreamName = streamName;
            ShardIteratorType = ShardIteratorType.LATEST;
            _credentials = credentials;
            _dataHandler = dataHandler;
        }

        private bool IsRunning { get; set; }
        private string StreamName { get; set; }
        private bool IsDebug { get; set; }

        public ShardIteratorType ShardIteratorType { get; set; }

        public void Start()
        {
            Action updateData = () =>
            {
                UpdateData();
            };

            if (!IsRunning)
            {
                IsRunning = true;
                Task.Factory.StartNew(updateData);
            }
        }

        public void Stop()
        {
            IsRunning = false;
        }

        private void UpdateData()
        {
            using (IAmazonKinesis klient = new AmazonKinesisClient(_credentials, Amazon.RegionEndpoint.USWest2))
            {
                ListStreamsResponse resp = klient.ListStreams();
                DescribeStreamRequest describeRequest = new DescribeStreamRequest();
                describeRequest.StreamName = StreamName;

                DescribeStreamResponse describeResponse = klient.DescribeStream(describeRequest);
                List<Shard> shards = describeResponse.StreamDescription.Shards;

                foreach (Shard shard in shards)
                {
                    SequenceNumberRange range = shard.SequenceNumberRange;

                    GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest();
                    iteratorRequest.StreamName = StreamName;
                    iteratorRequest.ShardId = shard.ShardId;
                    iteratorRequest.ShardIteratorType = ShardIteratorType;

                    GetShardIteratorResponse iteratorResponse = klient.GetShardIterator(iteratorRequest);
                    string iteratorId = iteratorResponse.ShardIterator;
                    while (IsRunning && !string.IsNullOrEmpty(iteratorId))
                    {
                        Thread.Sleep(1);
                        GetRecordsRequest getRequest = new GetRecordsRequest();
                        getRequest.Limit = 1000;
                        getRequest.ShardIterator = iteratorId;

                        GetRecordsResponse getResponse = klient.GetRecords(getRequest);
                        string nextIterator = getResponse.NextShardIterator;
                        List<Amazon.Kinesis.Model.Record> records = getResponse.Records;

                        if (records.Count > 0)
                        {
                            if (IsDebug)
                            {
                                string message = string.Format("Received {0} records. ", records.Count);
                                Console.WriteLine(message);
                                foreach(IDebugObserver observer in _debugObservers)
                                {
                                    observer.WriteDebug(message);
                                }
                            }
                            foreach (Amazon.Kinesis.Model.Record record in records)
                            {
                                if (!IsRunning)
                                {
                                    break;
                                }
                                Thread.Sleep(1);
                                string json = Encoding.UTF8.GetString(record.Data.ToArray());
                                if (IsDebug)
                                {
                                    string message = "Json string: " + json;
                                    Console.WriteLine(message);
                                    foreach (IDebugObserver observer in _debugObservers)
                                    {
                                        observer.WriteDebug(message);
                                    }
                                }
                                T obj = JsonConvert.DeserializeObject<T>(json);
                                if (obj != null && _dataHandler != null)
                                {
                                    _dataHandler(obj);
                                }
                            }
                        }
                        iteratorId = nextIterator;
                    }

                    if (!IsRunning)
                    {
                        break;
                    }

                }
            }

        }

        public void SetDebug(bool isDebug)
        {
            IsDebug = isDebug;
        }

        private List<IDebugObserver> _debugObservers = new List<IDebugObserver>();

        public void Register(IDebugObserver observer)
        {
            if (!_debugObservers.Contains(observer))
            {
                _debugObservers.Add(observer);
            }
        }

        public void Unregister(IDebugObserver observer)
        {
            if (_debugObservers.Contains(observer))
            {
                _debugObservers.Remove(observer);
            }
        }
    }
}
