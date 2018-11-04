using System;
using System.Threading;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Kafka.Config;
using Rebus.Logging;
using Rebus.Routing.TypeBased;
using Rebus.Workers.ThreadPoolBased;
using Shouldly;

namespace Rebus.Kafka.Tests
{
    [TestFixture]
    public class DeepTest
    {
        [Test]
        public void SendRec()
        {
            var input = "1";
            var finished = false;
            using (var adapter = new BuiltinHandlerActivator())
            {
                var y = 0;
                var z = 0;
                const int k = 1000;
                adapter.Handle<Job>(async reply =>
                {
                    await Console.Out.WriteLineAsync(
                        $"Got reply '{reply.data}' ");
                    input = reply.data;
                    if(y == k-1) finished = true;
                    y++;
                    z = z + int.Parse(reply.data);
                });

                Configure.With(adapter)
                    .Logging(l => l.ColoredConsole(minLevel: LogLevel.Warn))
                    .Routing(r => r.TypeBased().MapAssemblyOf<Job>("consumer.input"))
                    .Transport(t => t.UseKafka("127.0.0.1:9092","112", "consumer.input"))
                    .Start();
                var j = 0;
                for (var i = 1; i <= k; i++)
                {
                    adapter.Bus.Send(new Job(i.ToString())).Wait();
                    j = j + i;
                }
                
                var x = 0;
                while (!finished)
                {
                    if (x > 20) break;
                    x++;
                    Thread.Sleep(500);
                }
                adapter.Dispose();
                y.ShouldBe(k);
                z.ShouldBe(j);
                
            }
        }

        [Test]
        public void PubSubTest()
        {

        }
    }

    public class Job
    {
        public string data { get; set; }
        public Job(string input)
        {
            data = input;
        }
    }
}
