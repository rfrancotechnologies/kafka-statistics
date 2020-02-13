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
                var consumerConf = configuration.GetSection("consumerConf").Get<ConsumerConfig>();
                consumerConf.GroupId = Guid.NewGuid().ToString();
                
                ConsumerBuilder<Null, string> builder = new ConsumerBuilder<Null, string>(consumerConf);
                builder.SetErrorHandler((_, error) =>
                {
                    Console.WriteLine($"An error ocurred consuming the event: {error.Reason}");
                    if (error.IsFatal) Environment.Exit(-1);
                });
                
                builder.HandleStatistics(new PrometheusConsumerStatisticsHandler(new string[]{"application"}, new string[]{"test-consumer-statistics"} ));
                builder.SetKeyDeserializer(Deserializers.Null);
                builder.SetValueDeserializer(Deserializers.Utf8);

                using (var kafkaConsumer = builder.Build())
                {
                    kafkaConsumer.Subscribe(configuration.GetValue<string>("topic"));
                    while (!cancellationTokenSource.IsCancellationRequested)
                    {
                        ConsumeResult<Null, string> consumedResult;
                        try
                        {
                            consumedResult = kafkaConsumer.Consume(cancellationTokenSource.Token);
                            if(null != consumedResult)
                                Console.WriteLine($"Received message: {consumedResult.Value}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("An error occurred consuming the event.", ex);
                            Environment.Exit(-2);
                        }
                    }
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
