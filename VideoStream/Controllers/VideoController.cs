using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Hosting.Internal;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Win32.SafeHandles;

namespace VideoStream.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class VideoController : ControllerBase
    {
        private readonly ILogger<VideoController> _logger;

        private static IConsumer<Ignore, byte[]> _consumer;

       
        


        public VideoController(ILogger<VideoController> logger)
        {
            _logger = logger;


        }

        [HttpGet]
        public async Task GetLite()
        {
            //Response.ContentType = "video/mp4";
            Response.ContentType = "html/text";
            Response.StatusCode = 200;

            try
            {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = _consumer.Consume();

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                                continue;
                            }

                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}");
                            
                            await Response.Body.WriteAsync(consumeResult.Value,0,consumeResult.Value.Length);
                            await Response.Body.FlushAsync();
                            //if (consumeResult.Offset % 1024 == 0)
                            //{
                            //    writer.Flush();
                            //}

                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Closing consumer.");
                _consumer.Close();
            }



        }  




        [HttpGet]
        public async Task GetLive()
        {
            //Response.ContentType = "video/mp4";
            Response.ContentType = "html/text";
            Response.StatusCode = 200;

            //await GetVideo(Response.Body);

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

            consumer.Subscribe("video");

            try
            {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume();

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                                continue;
                            }

                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}");
                            await Response.WriteAsync($"Received message at {consumeResult.TopicPartitionOffset}");
                            //await Response.Body.WriteAsync(consumeResult.Value,0,consumeResult.Value.Length);
                            await Response.Body.FlushAsync();
                            //if (consumeResult.Offset % 1024 == 0)
                            //{
                            //    writer.Flush();
                            //}

                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Closing consumer.");
                consumer.Close();
            }



        }   

        public async Task SaveFile()
        {
            FileStream stream = new FileStream($@"D:\temp\video\{Guid.NewGuid().ToString()}.mp4",FileMode.CreateNew);

            await GetVideo(stream);
        }


        public async Task GetFile()
        {
            //Response.ContentType = "video/mp4";
            Response.ContentType = "html/text";
            Response.StatusCode = 200;

            //using (var inFileSteam = new FileStream(@"D:\Training\Kafka\WintellectNOW Architecting Distributed Cloud Applications Part 3 Messaging.mp4", FileMode.Open, FileAccess.Read))
            using (var inFileSteam = new FileStream(@"D:\Downloads\Telegram Desktop\VID-20191016-WA0002.mp4", FileMode.Open, FileAccess.Read))
            {
                byte[] buffer = new byte[1024]; // 5MB in bytes is 5 * 2^20
                int bytesRead = inFileSteam.Read(buffer, 0, buffer.Length);

                while (bytesRead > 0)
                {
                    await Response.Body.WriteAsync(buffer, 0, buffer.Length);
                    await Response.Body.FlushAsync();

                    await Task.Delay(1);

                    bytesRead = inFileSteam.Read(buffer, 0, buffer.Length);
                }

                inFileSteam.Close();
            }

        }

        [HttpGet]
        public async Task GetFileStream()
        {
            Response.ContentType = "video/mp4";
            Response.StatusCode = 200;
            var i = 0;
            while (true)
            {
                if (GlobalSteam.Queue.TryDequeue(out var buffer))
                {
                    await Response.Body.WriteAsync(buffer, 0, buffer.Length);
                    //Response.Body.Flush();
                    Console.WriteLine("Dequeue " + i++);
                }
                else
                {
                    Console.WriteLine("no item in queue");
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }
            }


            //GlobalSteam.WriteAsync = Response.Body.WriteAsync;
            //GlobalSteam.FlushAsync = Response.Body.FlushAsync;

            //GlobalSteam.Write = Response.Body.Write;
            //GlobalSteam.Flush = Response.Body.Flush;


            //byte[] buffer = new byte[1024]; // 5MB in bytes is 5 * 2^20

            //GlobalSteam.ExternalStream.Seek(0, SeekOrigin.Begin);

            //int bytesRead = GlobalSteam.ExternalStream.Read(buffer, 0, buffer.Length);

            //while (bytesRead > 0)
            //{
            //    Response.Body.Write(buffer, 0, buffer.Length);
            //    Response.Body.Flush();


            //    bytesRead = GlobalSteam.ExternalStream.Read(buffer, 0, buffer.Length);
            //}

            Thread.Sleep(TimeSpan.FromMinutes(10));
        }

        [HttpGet]
        public async Task SaveFileStream()
        {
            Response.StatusCode = 200;
            using (var inFileSteam = new FileStream(@"D:\Downloads\Telegram Desktop\VID-20191016-WA0002.mp4", FileMode.Open, FileAccess.Read))
            {
                byte[] buffer = new byte[1024]; // 5MB in bytes is 5 * 2^20
                int bytesRead = inFileSteam.Read(buffer, 0, buffer.Length);

                while (bytesRead > 0)
                {
                    GlobalSteam.ExternalStream.Write(buffer, 0, buffer.Length);
                    //GlobalSteam.ExternalStream.Flush();

                    //await GlobalSteam.WriteAsync(buffer, 0, buffer.Length);

                    //await GlobalSteam.FlushAsync();


                    bytesRead = inFileSteam.Read(buffer, 0, buffer.Length);
                }

                

                inFileSteam.Close();
            }

        }

        public async Task GetVideo(Stream writer)
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

            consumer.Subscribe("video");

            try
            {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume();

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                                writer.Close();
                                continue;
                            }

                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}");
                            
                            await writer.WriteAsync(consumeResult.Value,0,consumeResult.Value.Length);
                            await writer.FlushAsync();
                            //if (consumeResult.Offset % 1024 == 0)
                            //{
                            //    writer.Flush();
                            //}

                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Closing consumer.");
                consumer.Close();
            }

        }

        [HttpGet]
        public ActionResult Init()
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
            _consumer = new ConsumerBuilder<Ignore, byte[]>(config)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                // .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]");
                    // possibly manually specify start offsets or override the partition assignment provided by
                    // the consumer group by returning a list of topic/partition/offsets to assign to, e.g.:
                    // 
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    Console.WriteLine($"Revoking assignment: [{string.Join(", ", partitions)}]");
                })
                .Build();

            _consumer.Subscribe("video");

            var topic = _consumer.Assignment.FirstOrDefault()?.Topic;

            return Content("init - " + topic);
        }


    }
}
