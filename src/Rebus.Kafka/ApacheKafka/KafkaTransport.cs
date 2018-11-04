using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Kafka.Config;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Threading;
using Rebus.Transport;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Rebus.Kafka.Exceptions;
using Rebus.Kafka.Serialization;
using Rebus.Serialization;
using Rebus.Subscriptions;

namespace Rebus.Kafka.ApacheKafka
{
    public class KafkaTransport : ITransport, IInitializable, IDisposable, ISubscriptionStorage
    {
        readonly ILog _log;
        readonly IAsyncTaskFactory _asyncTaskFactory;
        private Producer<string, TransportMessage> _producer;
        private Consumer<Ignore, TransportMessage> _consumer;
        private string _brokerList;
        private ISerializer _customSerializer;
        private string _groupId;
        private ConcurrentBag<string> _knownRoutes;
        private object _routeLock= new object();
        private readonly string _topicPrefix;

        public KafkaTransport(
            IRebusLoggerFactory rebusLoggerFactory, 
            IAsyncTaskFactory asyncTaskFactory, 
            ISerializer customSerializer, 
            string brokerList,
            string groupId, 
            string topicPrefix)
        {
            _log = rebusLoggerFactory.GetLogger<KafkaTransport>();
            _brokerList = brokerList;
            _customSerializer = customSerializer;
            _groupId = groupId;
            _knownRoutes = new ConcurrentBag<string>();
            _knownRoutes.Add($"^{topicPrefix}.*");
            _knownRoutes.Add($"{topicPrefix}");
            _topicPrefix = topicPrefix;
            _asyncTaskFactory = asyncTaskFactory ?? throw new ArgumentNullException(nameof(asyncTaskFactory));
        }

        public void CreateQueue(string address)
        {
            _knownRoutes.Add(address);
            // auto create topics should be enabled
            _consumer.Subscribe(_knownRoutes);
        }

        public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            if (destinationAddress == null) throw new ArgumentNullException(nameof(destinationAddress));
            if (message == null) throw new ArgumentNullException(nameof(message));
            if (context == null) throw new ArgumentNullException(nameof(context));
            //if (!destinationAddress.StartsWith(_topicPrefix))
                //destinationAddress = $"{destinationAddress}";
            
            await _producer.ProduceAsync(destinationAddress,
                null,
                message);
        }

        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            return !_consumer.Consume(out var Message, 1000) ? null : Message.Value;
        }

        public string Address => $"{_topicPrefix}_";

        public void Initialize()
        {
            var configProducer = new Dictionary<string, object> { { "bootstrap.servers", _brokerList } };
            _producer = new Producer<string, TransportMessage>(configProducer, new StringSerializer(Encoding.UTF8), new TransportMessageSerializer());
            var configConsumer = new Dictionary<string, object>
            {
                { "bootstrap.servers", _brokerList },
                { "group.id", _groupId },
                { "enable.auto.commit", true }
            };
            _consumer =
                new Consumer<Ignore, TransportMessage>(configConsumer, null, new TransportMessageDeserializer());
            _consumer.Subscribe(_knownRoutes);
        }

        public void Dispose()
        {
            _producer.Flush(5);
            _producer.Dispose();
            _consumer.Dispose();
        }

        public async Task<string[]> GetSubscriberAddresses(string topic)
        {
            return new[] { $"{_topicPrefix}_{topic}" };
        }

        public async Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            _knownRoutes.Add($"{_topicPrefix}_{topic}");
            _consumer.Subscribe(_knownRoutes);
        }

        public async Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            lock (_routeLock)
            {
                var del = _knownRoutes.ToList();
                del.Remove($"{_topicPrefix}_{topic}");
                _knownRoutes = new ConcurrentBag<string>(del);
            }
            _consumer.Subscribe(_knownRoutes);
        }

        public bool IsCentralized => true;
    }
}
