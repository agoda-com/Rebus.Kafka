using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Kafka.Config;
using Rebus.Logging;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.Kafka.Tests.Factories
{
    class KafkaFactory : IBusFactory
    {
        readonly List<IDisposable> _stuffToDispose = new List<IDisposable>();

        public IBus GetBus<TMessage>(string inputQueueAddress, Func<TMessage, Task> handler)
        {
            var builtinHandlerActivator = new BuiltinHandlerActivator();

            builtinHandlerActivator.Handle(handler);

            var queueName = TestConfig.GetName(inputQueueAddress);

            var bus = Configure.With(builtinHandlerActivator)
                .Logging(l => l.ColoredConsole(minLevel: LogLevel.Warn))
                .Routing(r => r.TypeBased().MapAssemblyOf<Job>("consumer.input"))
                .Transport(t => t.UseKafka("127.0.0.1:9092", "1", "myTopics"))
                .Start();
            _stuffToDispose.Add(bus);
            return bus;
        }

        public void Cleanup()
        {
            _stuffToDispose.ForEach(d => d.Dispose());
            _stuffToDispose.Clear();
        }
    }
}
