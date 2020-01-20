using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace VideoStream
{
    public class KafkaSubscriber : BackgroundService
    {
        private readonly ILogger _logger;


        public KafkaSubscriber(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger(nameof(KafkaSubscriber));
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "10.104.51.12:9092,10.104.51.13:9092,10.104.51.14:9092",
                GroupId = "group",
                EnablePartitionEof = true
            };

            // Note: If a key or value deserializer is not set (as is the case below), the 
            // deserializer corresponding to the appropriate type from Confluent.Kafka.Deserializers
            // will be used automatically (where available). The default deserializer for string
            // is UTF8. The default deserializer for Ignore returns null for all input data
            // (including non-null data).
            var consumer = new ConsumerBuilder<Ignore, byte[]>(config)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => _logger.LogError($"Error: {e.Reason}"))
                // .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    _logger.LogInformation($"Assigned partitions: [{string.Join(", ", partitions)}]");
                    // possibly manually specify start offsets or override the partition assignment provided by
                    // the consumer group by returning a list of topic/partition/offsets to assign to, e.g.:
                    // 
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    _logger.LogInformation($"Revoking assignment: [{string.Join(", ", partitions)}]");
                })
                .Build();

            // consumer.Subscribe(_kafkaConfig.Topics);
            consumer.Subscribe("video");

            Task.Run(async () => await Consume(consumer, stoppingToken), stoppingToken);

            return Task.CompletedTask;
        }

        private async Task Consume(IConsumer<Ignore, byte[]> consumer, CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);

                        if (consumeResult.IsPartitionEOF)
                        {
                            _logger.LogTrace(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                            continue;
                        }

                        Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}");

                        GlobalSteam.Queue.Enqueue(consumeResult.Value);
                        //GlobalSteam.RichQueue.Enqueue(Tuple.Create(
                        //    consumeResult.TopicPartitionOffset.Offset.Value,
                        //    consumeResult.Value));


                        //GlobalSteam.ExternalStream.Write(consumeResult.Value, 0, consumeResult.Value.Length);
                        //GlobalSteam.ExternalStream.Flush();

                        //GlobalSteam.Write(consumeResult.Value, 0, consumeResult.Value.Length);
                        //await GlobalSteam.WriteAsync(consumeResult.Value, 0, consumeResult.Value.Length);

                        //if (consumeResult.Offset % 1024== 0)
                        //{ GlobalSteam.Flush(); }
                        //await _handler.HandleMessage(consumeResult, cancellationToken);


                    }
                    catch (ConsumeException e)
                    {
                        _logger.LogError($"Consume error: {e.Error.Reason}");
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e, $"Error occurred on consumer handler Method.");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Closing consumer.");
                consumer.Close();
            }
        }

    }
}

