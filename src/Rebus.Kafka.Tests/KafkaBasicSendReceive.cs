using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Kafka.Tests.Factories;
using Rebus.Messages;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Transports;
using Rebus.Transport;

namespace Rebus.Kafka.Tests
{
    public class KafkaBasicSendReceive : BasicSendReceive<KafkaTransportFactory>
    {
        readonly Encoding _defaultEncoding = Encoding.UTF8;

        [Test]
        public async Task CanSendAndReceive2()
        {
            var _cancellationToken = new CancellationTokenSource().Token;
            var _factory = new KafkaTransportFactory();
            var input1QueueName = TestConfig.GetName("input1");
            var input2QueueName = TestConfig.GetName("input2");

            var input1 = _factory.Create(input1QueueName);
            var input2 = _factory.Create(input2QueueName);

            await WithContext(async context =>
            {
                await input1.Send(input2QueueName, MessageWith("hej"), context);
            });

            await WithContext(async context =>
            {
                var transportMessage = await input2.Receive(context, _cancellationToken);
                var stringBody = GetStringBody(transportMessage);

                Assert.That(stringBody, Is.EqualTo("hej"));
            });
        }
        TransportMessage MessageWith(string stringBody)
        {
            var headers = new Dictionary<string, string>
            {
                {Headers.MessageId, Guid.NewGuid().ToString()}
            };
            var body = _defaultEncoding.GetBytes(stringBody);
            return new TransportMessage(headers, body);
        }
        string GetStringBody(TransportMessage transportMessage)
        {
            if (transportMessage == null)
            {
                throw new InvalidOperationException("Cannot get string body out of null message!");
            }

            return _defaultEncoding.GetString(transportMessage.Body);
        }
        async Task WithContext(Func<ITransactionContext, Task> contextAction, bool completeTransaction = true)
        {
            using (var scope = new RebusTransactionScope())
            {
                await contextAction(scope.TransactionContext);

                if (completeTransaction)
                {
                    await scope.CompleteAsync();
                }
            }
        }
    }
}
