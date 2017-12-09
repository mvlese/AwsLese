using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Newtonsoft.Json;
using System.Windows.Forms.DataVisualization;
using System.Windows.Forms.DataVisualization.Charting;

namespace AwsLese
{
    public partial class Form1 : Form, IDebugObserver
    {
        static string bucketName = "lese-temperature-pressure";
        static IAmazonS3 client;
        private Queue<TPCorrelation> _dataQueue;
        private Queue<TPAverage> _dataQueueAvg;
        private static object _lockObject;
        private const int MaxChartItems = 900;
        private Amazon.Runtime.AWSCredentials _credentials;
        private StreamConsumer<TPCorrelation> _tpCorrelationConsumer;
        private StreamConsumer<TPAverage> _tpAverageConsumer;

        private BindingList<BindingList<double>> bindingList { get; set; } = new BindingList<BindingList<double>>();
        private BindingList<double> values1 { get; set; } = new BindingList<double>();
        private BindingList<double> values2 { get; set; } = new BindingList<double>();
        private BindingList<double> values3 { get; set; } = new BindingList<double>();
        private BindingList<double> valuesAvg1 { get; set; } = new BindingList<double>();
        private BindingList<double> valuesAvg2 { get; set; } = new BindingList<double>();
        private BindingList<double> valuesAvg3 { get; set; } = new BindingList<double>();
        private BindingList<double> valuesAvg4 { get; set; } = new BindingList<double>();

        public Form1()
        {
            InitializeComponent();
        }

        private bool IsReading { get; set; }

        private void Form1_Load(object sender, EventArgs e)
        {
            //string data = ReadObjectData();
            _lockObject = new object();
            _dataQueue = new Queue<TPCorrelation>();
            _dataQueueAvg = new Queue<TPAverage>();

            values1.ListChanged += valuesChanged;
            //values2.ListChanged += valuesChanged;
            //values3.ListChanged += valuesChanged;

            bindingList.Add(values1);
            bindingList.Add(values2);
            bindingList.Add(values3);

            IsReading = false;

            chart1.GetToolTipText += this.chart1_GetToolTipText;
            AverageTemperatureChart.GetToolTipText += this.chart1_GetToolTipText;
            AveragePressureChart.GetToolTipText += this.chart1_GetToolTipText;
            AverageHumidityChart.GetToolTipText += this.chart1_GetToolTipText;
            AverageLuxChart.GetToolTipText += this.chart1_GetToolTipText;
            TemperaturePressureScatterChart.GetToolTipText += this.chart1_GetToolTipText;

            chart1.ChartAreas[0].AxisY.Minimum = -1.0;
            chart1.ChartAreas[0].AxisY.Maximum = 1.0;
            chart1.ChartAreas[0].AxisY.MajorTickMark.Interval = 0.1;

            chart1.Series[0].ChartType = SeriesChartType.Line;
            chart1.Series[0].Name = "Correlation Temp-Lux";

            chart1.Series[1].ChartType = SeriesChartType.Line;
            chart1.Series[1].Name = "Correlation Temp-Pressure";

            chart1.Series[2].ChartType = SeriesChartType.Line;
            chart1.Series[2].Name = "Correlation Temp-Humidity";

            lblMostRecentDataPointTs.Text = "";

            Amazon.Runtime.CredentialManagement.SharedCredentialsFile credFile = 
                new Amazon.Runtime.CredentialManagement.SharedCredentialsFile(@"C:\Users\mvlese\.aws\credentials");
            Amazon.Runtime.CredentialManagement.CredentialProfile prof;
            credFile.TryGetProfile("default", out prof);
            _credentials = prof.GetAWSCredentials(credFile);

            _tpCorrelationConsumer =
                new StreamConsumer<TPCorrelation>("lese_temperature_pressure_json", _credentials, CorrelationHandler);
            _tpAverageConsumer =
                new StreamConsumer<TPAverage>("lese_averages_json", _credentials, AverageHandler);

            _tpCorrelationConsumer.Register(this);
            _tpAverageConsumer.Register(this);

        }

        private void btnStart_Click(object sender, EventArgs e)
        {
            groupBox1.Enabled = false;
            btnStart.Enabled = false;
            btnStop.Enabled = true;
            btnClear.Enabled = false;

            ShardIteratorType iteratorType = ShardIteratorType.LATEST;
            if(rbStartOfStream.Checked)
            {
                iteratorType = ShardIteratorType.TRIM_HORIZON;
            }

            if (_tpCorrelationConsumer.ShardIteratorType != iteratorType)
            {
                Clear();
            }

            _tpAverageConsumer.ShardIteratorType = iteratorType;
            _tpCorrelationConsumer.ShardIteratorType = iteratorType;

            IsReading = true;
            Read();
        }

        private void btnStop_Click(object sender, EventArgs e)
        {
            groupBox1.Enabled = true;
            btnStart.Enabled = true;
            btnStop.Enabled = false;
            btnClear.Enabled = true;
            IsReading = false;
            _tpCorrelationConsumer.Stop();
            _tpAverageConsumer.Stop();
        }

        private void Clear()
        {
            _dataQueue.Clear();
            _dataQueueAvg.Clear();
            valuesAvg4.Clear();
            valuesAvg3.Clear();
            valuesAvg2.Clear();
            valuesAvg1.Clear();
            values3.Clear();
            values2.Clear();
            values1.Clear();
            handleValuesChanged();
        }

        private void btnClear_Click(object sender, EventArgs e)
        {
            Clear();
        }

        private void handleValuesChanged()
        {
            Action x = () =>
            {
                chart1.Series[0].Points.DataBindY(values1);
                chart1.Series[1].Points.DataBindY(values2);
                chart1.Series[2].Points.DataBindY(values3);

                AverageTemperatureChart.Series[0].Points.DataBindY(valuesAvg1);
                AveragePressureChart.Series[0].Points.DataBindY(valuesAvg2);
                AverageHumidityChart.Series[0].Points.DataBindY(valuesAvg3);
                AverageLuxChart.Series[0].Points.DataBindY(valuesAvg4);

                if (valuesAvg1.Count > valuesAvg2.Count)
                {
                    while (valuesAvg1 != valuesAvg2 && valuesAvg1.Count > 0)
                    {
                        valuesAvg1.RemoveAt(0);
                    }
                }
                else if (valuesAvg1.Count < valuesAvg2.Count)
                {
                    while (valuesAvg1 != valuesAvg2 && valuesAvg2.Count > 0)
                    {
                        valuesAvg2.RemoveAt(0);
                    }
                }

                if (valuesAvg1.Count == valuesAvg2.Count)
                {
                    TemperaturePressureScatterChart.Series[0].Points.DataBindXY(valuesAvg2, valuesAvg1);
                }
            };

            chart1.Invoke(x);
        }

        private void valuesChanged(object sender, ListChangedEventArgs args)
        {
            if (args.ListChangedType == ListChangedType.ItemAdded)
            {
                handleValuesChanged();
            }
        }

        private void CorrelationHandler(TPCorrelation value)
        {
            lock(_lockObject)
            {
                _dataQueue.Enqueue(value);
            }
        }

        private void AverageHandler(TPAverage value)
        {
            lock (_lockObject)
            {
                _dataQueueAvg.Enqueue(value);
            }
        }

        private string ReadObjectData()
        {
            string responseBody = "";

            using (client = new AmazonS3Client(_credentials, Amazon.RegionEndpoint.USWest2))
            {
                ListBucketsResponse lbr = client.ListBuckets();

                IList<string> objectKeys = client.GetAllObjectKeys(bucketName, "2017", null);
                foreach (string keyName in objectKeys)
                {
                    try
                    {
                        GetObjectRequest request = new GetObjectRequest
                        {
                            BucketName = bucketName,
                            Key = keyName
                        };
                    
                        using (GetObjectResponse response = client.GetObject(request))
                        {
                            using (Stream responseStream = response.ResponseStream)
                            {
                                using (StreamReader reader = new StreamReader(responseStream))
                                {
                                    string title = response.Metadata["x-amz-meta-title"];
                                    Console.WriteLine("The object's title is {0}", title);

                                    responseBody = reader.ReadToEnd();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                }
            }
            return responseBody;
        }

        private void Read()
        {
            Action updateData = () =>
            {
                UpdateData();
            };

            Action updateChart = () =>
            {
                UpdateChart();
            };

            _tpCorrelationConsumer.Start();
            _tpAverageConsumer.Start();
            // Task.Factory.StartNew(updateData);
            Task.Factory.StartNew(updateChart);
        }

        private void UpdateChart()
        {
            while (IsReading)
            {
                BindingList<double> list = null;

                Thread.Sleep(1);
                if (_dataQueue.Count > 0)
                {
                    list = null;
                    TPCorrelation tpCorrelation = null;
                    lock (_lockObject)
                    {
                        tpCorrelation = _dataQueue.Dequeue();
                    }
                    if (tpCorrelation.correlation_coefficient.HasValue)
                    {
                        if (string.Compare(tpCorrelation.kind.Trim(), "temperature-lux", StringComparison.OrdinalIgnoreCase) == 0)
                        {
                            list = values1;
                        }
                        else if (string.Compare(tpCorrelation.kind.Trim(), "temperature-pressure", StringComparison.OrdinalIgnoreCase) == 0)
                        {
                            list = values2;
                        }
                        else if (string.Compare(tpCorrelation.kind.Trim(), "temperature-humidity", StringComparison.OrdinalIgnoreCase) == 0)
                        {
                            list = values3;
                        }
                    }

                    if (list != null)
                    {
                        if (list.Count >= MaxChartItems)
                        {
                            list.RemoveAt(0);
                        }
                        list.Add(tpCorrelation.correlation_coefficient.Value);
                    }

                    if (tpCorrelation.ts.HasValue)
                    {
                        Action x = () =>
                        {
                            lblMostRecentDataPointTs.Text = tpCorrelation.ts.Value.ToLocalTime().ToString("yyyy-MM-dd hh:mm:ss");
                        };
                        lblMostRecentDataPointTs.Invoke(x);
                    }
                }

                if (_dataQueueAvg.Count > 0)
                {
                    list = null;
                    TPAverage tpAverage = null;
                    lock (_lockObject)
                    {
                        tpAverage = _dataQueueAvg.Dequeue();
                    }
                    if (tpAverage.kind.Trim().CompareTo("temperature") == 0)
                    {
                        list = valuesAvg1;
                    }
                    else if (tpAverage.kind.Trim().CompareTo("pressure") == 0)
                    {
                        list = valuesAvg2;
                    }
                    else if (tpAverage.kind.Trim().CompareTo("humidity") == 0)
                    {
                        list = valuesAvg3;
                    }
                    else if (tpAverage.kind.Trim().CompareTo("lux") == 0)
                    {
                        list = valuesAvg4;
                    }

                    if (list != null)
                    {
                        if (list.Count >= MaxChartItems)
                        {
                            list.RemoveAt(0);
                        }
                        list.Add(tpAverage.average.Value);
                    }
                }
            }
        }

        private void UpdateData()
        {
            using (IAmazonKinesis klient = new AmazonKinesisClient(_credentials, Amazon.RegionEndpoint.USWest2))
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

                    while (IsReading && !string.IsNullOrEmpty(iteratorId))
                    {
                        Thread.Sleep(1);
                        GetRecordsRequest getRequest = new GetRecordsRequest();
                        getRequest.Limit = 10000;
                        getRequest.ShardIterator = iteratorId;

                        GetRecordsResponse getResponse = klient.GetRecords(getRequest);
                        string nextIterator = getResponse.NextShardIterator;
                        List<Record> records = getResponse.Records;

                        if (records.Count > 0)
                        {
                            // Console.WriteLine("Received {0} records. ", records.Count);
                            foreach (Record record in records)
                            {
                                if (!IsReading)
                                {
                                    break;
                                }
                                Thread.Sleep(1);
                                string json = Encoding.UTF8.GetString(record.Data.ToArray());
                                // Console.WriteLine("Json string: " + json);
                                TPCorrelation tpCorr = JsonConvert.DeserializeObject<TPCorrelation>(json);
                                lock (_lockObject)
                                {
                                    _dataQueue.Enqueue(tpCorr);
                                }
                            }
                        }
                        iteratorId = nextIterator;
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
        class TPCorrelation
        {
            public DateTime? ts { get; set; }
            public string kind { get; set; }
            public float? correlation_coefficient { get; set; }
            public float? stdev_temperature { get; set; }
            public float? stdev_other { get; set; }
        }

        class TPAverage
        {
            public DateTime? ts { get; set; }
            public string kind { get; set; }
            public float? average { get; set; }
        }

        private void checkBox1_CheckedChanged(object sender, EventArgs e)
        {
            _tpAverageConsumer.SetDebug(checkBox1.Checked);
            _tpCorrelationConsumer.SetDebug(checkBox1.Checked);
        }

        private Queue<string> _debugQueue = new Queue<string>();

        public void WriteDebug(string message)
        {
            //_debugQueue.Enqueue(message);
            //if (_debugQueue.Count > 1000)
            //{
            //    _debugQueue.Dequeue();
            //}

            Action x = () =>
            {
                //richTextBox1.Lines = _debugQueue.ToArray();
                //// set the current caret position to the end
                //richTextBox1.SelectionStart = richTextBox1.Text.Length;
                //// scroll it automatically
                //richTextBox1.ScrollToCaret();
                Thread.Sleep(1);
                richTextBox1.Focus();
                richTextBox1.AppendText(message + Environment.NewLine);
            };

            richTextBox1.Invoke(x);
        }

        private void chart1_GetToolTipText(object sender, ToolTipEventArgs e)
        {
            // Check selected chart element and set tooltip text for it
            switch (e.HitTestResult.ChartElementType)
            {
                case ChartElementType.DataPoint:
                    var dataPoint = e.HitTestResult.Series.Points[e.HitTestResult.PointIndex];
                    e.Text = string.Format("X: {0}   Y: {1}", dataPoint.XValue, dataPoint.YValues[0]);
                    break;
            }
        }
    }
}
