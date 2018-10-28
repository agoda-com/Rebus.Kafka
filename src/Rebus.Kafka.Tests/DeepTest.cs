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
            string input = "1";
            bool finished = false;
            using (var adapter = new BuiltinHandlerActivator())
            {
                adapter.Handle<Job>(async reply =>
                {
                    await Console.Out.WriteLineAsync(
                        $"Got reply '{reply.data}' ");
                    input = reply.data;
                    finished = true;
                });

                Configure.With(adapter)
                    .Logging(l => l.ColoredConsole(minLevel: LogLevel.Warn))
                    .Routing(r => r.TypeBased().MapAssemblyOf<Job>("consumer.input"))
                    .Transport(t => t.UseKafka("127.0.0.1:9092","1"))
                    .Start();
                adapter.Bus.Send(new Job("0")).Wait();
                var x = 0;
                while (!finished)
                {
                    if(x >50) throw new Exception("Timeout waiting for response from Kafka");
                    x++;
                    Thread.Sleep(500);
                }
                input.ShouldBe("0");
            }
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
