using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace VideoStream.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class Uploader : Controller
    {
        public void Upload()
        {
            IProducer<Null, byte[]> producer = 
                new ProducerBuilder<Null, byte[]>(new ProducerConfig { BootstrapServers = "10.104.51.12:9092,10.104.51.13:9092,10.104.51.14:9092" })
                    .Build();

            using (var inFileSteam = new FileStream(@"D:\Downloads\Telegram Desktop\VID-20191016-WA0002.mp4", FileMode.Open, FileAccess.Read))
                // using (var inFileSteam = new FileStream(@"C:\Users\Soren\OneDrive\listen learned\ApacheKafka.txt", FileMode.Open))
            {
                byte[] buffer = new byte[1024]; // 5MB in bytes is 5 * 2^20
                int bytesRead = inFileSteam.Read(buffer, 0, buffer.Length);

                while (bytesRead > 0)
                {
                    producer.Produce(
                        "video",
                        new Message<Null, byte[]> { Value = buffer });

                    bytesRead = inFileSteam.Read(buffer, 0, buffer.Length);
                }
            }
        }
    }
}
