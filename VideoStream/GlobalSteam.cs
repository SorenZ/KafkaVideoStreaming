using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace VideoStream
{
    public class GlobalSteam
    {

        public static Action<byte[] , int , int> Write;
        public static Action Flush;

        public static Func<byte[] , int , int, Task> WriteAsync;
        public static Func<Task> FlushAsync;

        public static MemoryStream ExternalStream = new MemoryStream(1024 * 1024);
        public static ConcurrentQueue<byte[]> Queue = new ConcurrentQueue<byte[]>();
        public static ConcurrentQueue<Tuple<long, byte[]>> RichQueue = new ConcurrentQueue<Tuple<long,byte[]>>();


        public static void HandleMessage(Stream internalStream)
        {
            byte[] buffer = new byte[1024]; // 5MB in bytes is 5 * 2^20
            int bytesRead = ExternalStream.Read(buffer, 0, buffer.Length);

            while (bytesRead > 0)
            {
                internalStream.WriteAsync(buffer, 0, buffer.Length);
                internalStream.FlushAsync();

                bytesRead = ExternalStream.Read(buffer, 0, buffer.Length);
            }
        }
    }
}
