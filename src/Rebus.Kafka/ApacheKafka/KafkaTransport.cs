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
            ConcurrentBag<string> knownRoutes,
            string topicPrefix)
        {
            _log = rebusLoggerFactory.GetLogger<KafkaTransport>();
            _brokerList = brokerList;
            _customSerializer = customSerializer;
            _groupId = groupId;
            _knownRoutes = knownRoutes;
            _topicPrefix = topicPrefix;
            _asyncTaskFactory = asyncTaskFactory ?? throw new ArgumentNullException(nameof(asyncTaskFactory));
        }

        public void CreateQueue(string address)
        {
            _knownRoutes.Add(address);
            // auto create topics should be enabled
            _consumer.Subscribe(_knownRoutes);
        }

        private const string CurrentTransactionKey = "apache-confluant-kafka-transation";

        public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            if (destinationAddress == null) throw new ArgumentNullException(nameof(destinationAddress));
            if (message == null) throw new ArgumentNullException(nameof(message));
            if (context == null) throw new ArgumentNullException(nameof(context));

            var outgoingMessages = context.GetOrAdd(CurrentTransactionKey, () =>
            {
                var messagesToSend = new ConcurrentQueue<OutgoingMessage>();
                context.OnCommitted(async () => await SendOutgoingMessages(messagesToSend));
                return messagesToSend;
            });
            //TODO this is messy
            await Task.Run(() => outgoingMessages.Enqueue(new OutgoingMessage(destinationAddress, message)));
        }

        async Task SendOutgoingMessages(ConcurrentQueue<OutgoingMessage> outgoingMessages)
        {
            //TODO need to do performance testing here
            await Task.WhenAll(
                outgoingMessages
                    .Select(async message =>
                        await _producer.ProduceAsync(message.DestinationAddress,
                            null,
                            message.TransportMessage))
                            );
        }
        class OutgoingMessage
        {
            public string DestinationAddress { get; }
            public TransportMessage TransportMessage { get; }

            public OutgoingMessage(string destinationAddress, TransportMessage transportMessage)
            {
                DestinationAddress = destinationAddress;
                TransportMessage = transportMessage;
            }
        }

        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            //TODO this is messy
            _consumer.Poll(500);
            return await Task.Run(() =>
            {
                _temporaryQueue.TryDequeue(out var output);
                return output;
            }, cancellationToken);
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
            _consumer.OnMessage += IncommingMessage;
            _consumer.Subscribe(_knownRoutes);
        }

        private ConcurrentQueue<TransportMessage> _temporaryQueue = new ConcurrentQueue<TransportMessage>();

        private void IncommingMessage(object sender, Message<Ignore, TransportMessage> e)
        {
            _temporaryQueue.Enqueue(e.Value);
        }

        public void Dispose()
        {
            _producer.Flush(5);
            _producer.Dispose();
            _consumer.Dispose();
            var x = 0;
            while (_temporaryQueue.Count != 0)
            {
                x++;
                if (x > 15)
                {
                    throw new UnableToClearQueueException(){remainingMessages = _temporaryQueue.ToArray()};
                }
                Thread.Sleep(1);
            }
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
