using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rebus.Extensions;
using Rebus.Kafka.ApacheKafka;
using Rebus.Logging;
using Rebus.Tests.Contracts.Transports;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Transport;

namespace Rebus.Kafka.Tests.Factories
{
    public class KafkaTransportFactory : ITransportFactory
    {
        readonly Dictionary<string, KafkaTransport> _queuesToDelete = new Dictionary<string, KafkaTransport>();

        public ITransport CreateOneWayClient()
        {
            return Create(null);
        }

        public ITransport Create(string inputQueueAddress)
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);

            if (inputQueueAddress == null)
            {
                var transport = new KafkaTransport(consoleLoggerFactory,asyncTaskFactory,null, "127.0.0.1:9092", "1", inputQueueAddress);

                transport.Initialize();

                return transport;
            }

            return _queuesToDelete.GetOrAdd(inputQueueAddress, () =>
            {
                var transport = new KafkaTransport(consoleLoggerFactory, asyncTaskFactory, null, "127.0.0.1:9092", "1", inputQueueAddress);

                transport.Initialize();

                return transport;
            });
        }

        public void CleanUp()
        {
        }
    }
}
