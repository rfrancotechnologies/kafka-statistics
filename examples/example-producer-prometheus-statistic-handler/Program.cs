using System;
using System.Threading;
using Com.RFranco.Kafka.Statistics;
using Com.RFranco.Kafka.Statistics.Prometheus;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Prometheus;

namespace Com.Rfranco.TestKafkaStatistics
{
    class Program
    {
        public static void Main(string[] args)
        {

            var configuration = GetConfiguration(args);

            try
            {

                var prometheusConfig = configuration.GetSection("prometheusMetrics").Get<PrometheusConfig>();
                MetricServer metricServer = null;
                if (prometheusConfig.Enabled)
                {
                    metricServer = new MetricServer(port: prometheusConfig.Port);
                    metricServer.Start();
                }

                CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                ProducerBuilder<Null, string> builder = new ProducerBuilder<Null, string>(configuration.GetSection("producerConf").Get<ProducerConfig>());
                builder.SetErrorHandler((_, error) =>
                {
                    Console.WriteLine($"An error ocurred producing the event: {error.Reason}");
                    if (error.IsFatal) Environment.Exit(-1);
                });
                
                builder.HandleStatistics(new PrometheusProducerStatisticsHandler(new string[] { "application" }, new string[] { "test-producer-statistics" }));
                builder.SetKeySerializer(Serializers.Null);
                builder.SetValueSerializer(Serializers.Utf8);


                using (var producer = builder.Build())
                {
                    Action<DeliveryReport<Null, string>> handler = r =>
                    {
                        if (r.Error.IsError)
                        {
                            Console.WriteLine($"Delivery Error: {r.Error.Reason}");
                        }
                        else
                        {
                            Console.WriteLine($"Delivered message to {r.TopicPartitionOffset}");
                        }

                    };

                    int numMessages = 0;
                    while (!cancellationTokenSource.IsCancellationRequested)
                    {
                        try
                        {
                            var dr = producer.ProduceAsync(configuration.GetValue<string>("topic"), new Message<Null, string> { Value = $"message {numMessages}"});
                            Console.WriteLine($"Delivered  message {numMessages} : {dr.Result.Value}");
                            Thread.Sleep(1000);
                            numMessages++;
                        }
                        catch (ProduceException<Null, string> e)
                        {
                            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                        }
                    }

                    Console.WriteLine("Exit requested.");
                    producer.Flush(TimeSpan.FromSeconds(10));
                }

                Console.WriteLine("Exit requested. Gracefully exiting...");

            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred while starting up the test.", ex);
                Environment.Exit(-2);
            }
        }

        private static IConfiguration GetConfiguration(string[] args)
        {
            var configurationBuilder = new ConfigurationBuilder();
            configurationBuilder.AddJsonFile("config.json", optional: true, reloadOnChange: true);
            configurationBuilder.AddEnvironmentVariables();
            configurationBuilder.AddCommandLine(args);
            return configurationBuilder.Build();
        }
    }
}
