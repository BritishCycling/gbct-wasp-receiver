using Google.Api.Gax;
using Google.Cloud.Firestore;
using Google.Cloud.PubSub.V1;
using Newtonsoft.Json;
using NorthPoleEngineering.WaspClassLibrary;
// using NorthPoleEngineering.WaspClassLibrary.AeroSensorMonitor;
using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace WaspReceiver
{
    internal class Program
    {
        [FirestoreData]
        public struct TelemetryEvent
        {
            [FirestoreProperty]
            public DateTimeOffset? timestamp { get; set; }

            [FirestoreProperty]
            public uint? sensor_hardware_id { get; set; }

            [FirestoreProperty]
            public List<string> wasp_mac_addresses { get; set; }

            [FirestoreProperty]
            public string sensor_type { get; set; }

            [FirestoreProperty]
            public Dictionary<string, double> readings { get; set; }


            [FirestoreProperty]
            public DateTimeOffset? expiry_timestamp { get; set; }
        }

        public struct HealthCheckMessage
        {
            public long timestamp { get; set; }
            public int num_readings { get; set; }
            public int location_id { get; set; }
        }

        static ManualResetEvent _quitEvent = new ManualResetEvent(false);

        static int EXPIRY_YEARS = 2;
        static int MAX_SPEED = 30;
        static TimeSpan waspStartTime = new TimeSpan(7, 00, 0);
        static TimeSpan waspEndTime = new TimeSpan(21, 00, 0);
        static int healthCheckMillis = 1000 * 5 * 60;
        static int LOCATION_ID = 1;
        static int readingCount = 0;

        public static string LogLocation = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments),
        "WaspLogs", "SensorDemo-.log");

        static string fileOutTemp = "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {SourceContext}.{MemberName}{Message:l}{NewLine}{Exception}";
        static string conOutTemp = "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {SourceContext}.{MemberName}{Message:l}{NewLine}{Exception}";
        internal static ILogger DemoLogger = new LoggerConfiguration()
            .Enrich.FromLogContext()
            .WriteTo.Console(outputTemplate: conOutTemp)
            .WriteTo.File(LogLocation, outputTemplate: fileOutTemp, fileSizeLimitBytes: 10 * 1024 * 1024, rollingInterval: RollingInterval.Day, shared: true)
            .CreateLogger();
        static string topicId = "telemetry";
        static List<PublisherClient> publishers = SetUpPublishers();
        static List<PublisherClient> healthCheckPublishers = SetUpHealthCheckPublishers();
        static WaspCollection wasps = new WaspCollection();
        static System.Timers.Timer timer = new System.Timers.Timer(healthCheckMillis);

        static Task Main(string[] args)
        {
            Console.CancelKeyPress += (sender, eArgs) =>
            {
                Debug.WriteLine("Quitting");
                _quitEvent.Set();
                eArgs.Cancel = true;
            };
            Console.WriteLine(waspStartTime.ToString());
            Console.WriteLine(waspEndTime.ToString());

            timer.Elapsed += HealthCheck;
            timer.Enabled = true;
            Logger.LogLevel = Logger.LoggingLevel.Warning;
            AntPlusDevices ants = new AntPlusDevices(wasps);
            wasps.CollectionChanged += wasps_CollectionChanged;
            ants.AntPlusDeviceEvent += ants_AntPlusDeviceEvent;

            HealthCheck(null, null);

            _quitEvent.WaitOne();
            return Task.CompletedTask;
        }

        static protected List<PublisherClient> SetUpPublishers()
        {
            List<PublisherClient> pubSubsPublishers = new List<PublisherClient>();
            List<string> envs = new List<string> { "prod", "showcase" };

            foreach (string env in envs)
            {
                string projectId = $"gbct-{env}";
                TopicName topicName = new TopicName(projectId, topicId);
                PublisherClient.Settings customSettings = new PublisherClient.Settings
                {
                    BatchingSettings = new BatchingSettings(
                        elementCountThreshold: null,
                        byteCountThreshold: 1000,
                        delayThreshold: TimeSpan.FromMilliseconds(1000)
                    ),
                };
                PublisherClientBuilder publisherBuilder = new PublisherClientBuilder
                {
                    TopicName = topicName,
                    Settings = customSettings,
                };
                pubSubsPublishers.Add(publisherBuilder.Build());
                Console.WriteLine(pubSubsPublishers.Count.ToString());
            }
            return pubSubsPublishers;
        }

        static protected List<PublisherClient> SetUpHealthCheckPublishers()
        {
            List<PublisherClient> healthCheckPublishers = new List<PublisherClient>();
            List<string> envs = new List<string> { "prod", "showcase" };

            foreach (string env in envs)
            {
                string projectId = $"gbct-{env}";
                TopicName topicName = new TopicName(projectId, "gbct-telemetry-health-check");
                PublisherClient.Settings customSettings = new PublisherClient.Settings
                {
                    BatchingSettings = new BatchingSettings(
                        elementCountThreshold: null,
                        byteCountThreshold: 1000,
                        delayThreshold: TimeSpan.FromMilliseconds(1000)
                    ),
                };
                PublisherClientBuilder publisherBuilder = new PublisherClientBuilder
                {
                    TopicName = topicName,
                    Settings = customSettings,
                };
                healthCheckPublishers.Add(publisherBuilder.Build());
                Console.WriteLine(healthCheckPublishers.Count.ToString());
            }
            return healthCheckPublishers;
        }

        static void wasps_CollectionChanged(object sender, System.Collections.Specialized.NotifyCollectionChangedEventArgs e)
        {
            if (e.Action == System.Collections.Specialized.NotifyCollectionChangedAction.Add)
            {
                foreach (Wasp w in e.NewItems)
                {
                    Console.WriteLine(w.Name);
                    w.TimeoutInterval = 5;
                }

            }
            else
            {
                Console.WriteLine($"Other event {e.Action}");
            }
        }

        static void ants_AntPlusDeviceEvent(object sender, AntPlusDeviceEventArgs e)
        {
            switch (e.DeviceType)
            {
                case AntDeviceType.HeartRateMonitor:
                    ((HeartRateMonitor)(e.Device)).HeartRateMessageEvent += HeartRateEvent;
                    break;
                case AntDeviceType.BikeCadenceSensor:
                    ((BikeCadenceSensor)(e.Device)).BikeCadenceMessageEvent += CadenceEvent;
                    break;
                case AntDeviceType.BikeSpeedSensor:
                    ((BikeSpeedSensor)(e.Device)).BikeSpeedMessageEvent += SpeedEvent;
                    break;
                case AntDeviceType.BikeSpeedCadence:
                    ((BikeSpeedAndCadenceSensor)(e.Device)).BikeSpeedAndCadenceMessageEvent += SpeedCadenceEvent;
                    break;
                case AntDeviceType.BikePowerSensor:
                    ((BikePowerSensor)(e.Device)).BikePowerOnlyEvent += PowerEvent;
                    break;
                //case AntDeviceType.AeroSensor:
                //    ((AeroSensor)(e.Device)).DynamicPressureEvent += AeroSensorDynamicPressureEvent;
                //    break;
                default:
                    break;
            }
        }

        static async void HealthCheck(object sender, ElapsedEventArgs e)
        {
            var data = new HealthCheckMessage
            {
                timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                num_readings = readingCount,
                location_id = LOCATION_ID,
            };
            var json = JsonConvert.SerializeObject(data);
            Console.WriteLine(json);
            foreach (PublisherClient publisher in healthCheckPublishers)
            {
                await publisher.PublishAsync(json.ToString());
            }
            readingCount = 0;
        }

        static async Task SendEventAsync(DateTime timestamp, MessageEventArgs e, string sensor_type, Dictionary<string, double> readings)
        {
            if (readings["rssi"] == 1)
            {
                return;
            }
            TimeSpan timeOfDay = DateTime.Now.TimeOfDay;
            if (timeOfDay.CompareTo(waspStartTime) < 0 || timeOfDay.CompareTo(waspEndTime) > 0)
            {
                // Only upload signals between start and end times for system
                return;
            }

            string WaspIPAddress = e.WaspIPAddress.ToString();
            IEnumerable<Wasp> matchingWasps = wasps.Where(wasp => wasp.WaspIPAddress.ToString() == WaspIPAddress);
            string WaspMAC = wasps.Count > 0 ? BitConverter.ToString(wasps[0].MAC) : "Unknown";
            var ev = new TelemetryEvent
            {
                timestamp = timestamp,
                wasp_mac_addresses = new List<string>() { WaspMAC },
                sensor_hardware_id = e.ExtendedDeviceNumber,
                sensor_type = sensor_type,
                readings = readings,
                expiry_timestamp = timestamp.AddYears(EXPIRY_YEARS)
            };

            var json = JsonConvert.SerializeObject(ev);
            foreach (PublisherClient publisher in publishers)
            {
                await publisher.PublishAsync(json);
            }
            readingCount += 1;
        }

        //static async void AeroSensorDynamicPressureEvent(object sender, AeroSensorDynamicPressureMessageEventArgs eventArgs)
        //{
        //    if (eventArgs.DynamicPressure == 0 && eventArgs.YawAngle == 0)
        //    {
        //        return;
        //    }
        //    var timestamp = DateTime.UtcNow;
        //    var readings = new Dictionary<string, double>()
        //    {
        //        {"yaw_angle", eventArgs.YawAngle},
        //        {"air_density", eventArgs.AirDensity},
        //        {"dynamic_pressure", eventArgs.DynamicPressure},
        //        {"rssi", eventArgs.RssiValue},
        //    };

        //    await SendEventAsync(timestamp, eventArgs, "Air Speed", readings);
        //}

        static async void HeartRateEvent(object sender, HeartRateMonitorMessageEventArgs eventArgs)
        {
            var timestamp = DateTime.UtcNow;
            var readings = new Dictionary<string, double>()
            {
                {"heart_rate", double.Parse(eventArgs.FormattedHeartRate(false))},
                {"rssi", eventArgs.RssiValue},
            };
            await SendEventAsync(timestamp, eventArgs, "Heart Rate", readings);
        }

        static async void PowerEvent(object sender, PowerOnlyEventArgs eventArgs)
        {
            var timestamp = DateTime.UtcNow;
            var readings = new Dictionary<string, double>()
            {
                {"power", eventArgs.InstantaneousPower},
                {"power_balance", eventArgs.PedalPower},
                {"cadence", eventArgs.InstantaneousCadence},
                {"rssi", eventArgs.RssiValue},
            };
            await SendEventAsync(timestamp, eventArgs, "Power", readings);
        }

        static async void SpeedEvent(object sender, BikeSpeedMessageEventArgs eventArgs)
        {
            var timestamp = DateTime.UtcNow;
            var readings = new Dictionary<string, double>()
            {
                {"speed", eventArgs.InstantaneousSpeed},
                {"speed_rev_count", eventArgs.AccumulatedRevolutionCount},
                {"rssi", eventArgs.RssiValue},
            };
            if (eventArgs.InstantaneousSpeed > MAX_SPEED)
            {
                return;
            }
            await SendEventAsync(timestamp, eventArgs, "Speed", readings);
        }

        static async void SpeedCadenceEvent(object sender, BikeSpeedAndCadenceMessageEventArgs eventArgs)
        {
            var timestamp = DateTime.UtcNow;
            var readings = new Dictionary<string, double>()
            {
                {"speed", eventArgs.InstantaneousSpeed},
                {"speed_rev_count", eventArgs.AccumulatedSpeedRevolutionCount},
                {"cadence", eventArgs.InstantaneousCadence},
                {"cadence_rev_count", eventArgs.AccumulatedCadenceRevolutionCount},
                {"rssi", eventArgs.RssiValue},
            };
            await SendEventAsync(timestamp, eventArgs, "Speed Cadence", readings);
        }

        static async void CadenceEvent(object sender, BikeCadenceMessageEventArgs eventArgs)
        {
            var timestamp = DateTime.UtcNow;
            var readings = new Dictionary<string, double>()
            {
                {"cadence", eventArgs.InstantaneousCadence},
                {"cadence_rev_count", eventArgs.AccumulatedRevolutionCount},
                {"rssi", eventArgs.RssiValue},
            };
            await SendEventAsync(timestamp, eventArgs, "Cadence", readings);
        }
    }
}
