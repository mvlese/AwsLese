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

            bindingList.Add(values1);
            bindingList.Add(values2);
            bindingList.Add(values3);

            IsReading = false;

            chart1.GetToolTipText += chart1_GetToolTipText;
            AverageTemperatureChart.GetToolTipText += chart1_GetToolTipText;
            AveragePressureChart.GetToolTipText += chart1_GetToolTipText;
            AverageHumidityChart.GetToolTipText += chart1_GetToolTipText;
            AverageLuxChart.GetToolTipText += chart1_GetToolTipText;
            TemperaturePressureScatterChart.GetToolTipText += chart1_GetToolTipText;

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

                double tmin = 0;
                double tmax = 0;
                double pmin = 0;
                double pmax = 0;
                if (valuesAvg1.Count > 0)
                {
                    tmin = Math.Floor(valuesAvg1.Min());
                    tmax = Math.Ceiling(valuesAvg1.Max());
                    AverageTemperatureChart.ChartAreas[0].AxisY.Minimum = tmin;
                    AverageTemperatureChart.ChartAreas[0].AxisY.Maximum = tmax;
                }
                if (valuesAvg2.Count > 0)
                {
                    pmin = Math.Floor(valuesAvg2.Min());
                    pmax = Math.Ceiling(valuesAvg2.Max());
                    AveragePressureChart.ChartAreas[0].AxisY.Minimum = pmin;
                    AveragePressureChart.ChartAreas[0].AxisY.Maximum = pmax;
                }
                AverageTemperatureChart.Series[0].Points.DataBindY(valuesAvg1);
                AveragePressureChart.Series[0].Points.DataBindY(valuesAvg2);
                AverageHumidityChart.Series[0].Points.DataBindY(valuesAvg3);
                AverageLuxChart.Series[0].Points.DataBindY(valuesAvg4);

                if (valuesAvg1.Count > 100 && valuesAvg2.Count > 100 && valuesAvg1.Count == valuesAvg2.Count)
                {
                    TemperaturePressureScatterChart.ChartAreas[0].AxisY.Minimum = tmin;
                    TemperaturePressureScatterChart.ChartAreas[0].AxisY.Maximum = tmax;
                    TemperaturePressureScatterChart.ChartAreas[0].AxisX.Minimum = pmin;
                    TemperaturePressureScatterChart.ChartAreas[0].AxisX.Maximum = pmax;

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

        private void Read()
        {
            Action updateChart = () =>
            {
                UpdateChart();
            };

            _tpCorrelationConsumer.Start();
            _tpAverageConsumer.Start();
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
                        handleValuesChanged();
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
                        handleValuesChanged();
                    }
                }
            }
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
