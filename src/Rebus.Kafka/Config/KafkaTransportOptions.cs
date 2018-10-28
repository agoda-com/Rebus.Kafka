using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rebus.Config;
using Rebus.Kafka.ApacheKafka;
using Rebus.Logging;
using Rebus.Routing;
using Rebus.Threading;
using Rebus.Transport;

namespace Rebus.Kafka.Config
{
    public static class KafkaTransportOptions
    {
        public static void UseKafka(this StandardConfigurer<ITransport> configurer,
            string brokerList, string groupId)
        {
            configurer.Register(c =>
            {
                var router = c.Get<IRouter>();
                if(router == null) throw new Exception("Route not configured");
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
                var listOfTopics = router.GetListOfTopics();
                return new KafkaTransport(rebusLoggerFactory, asyncTaskFactory, null, brokerList, groupId, new ConcurrentBag<string>(listOfTopics));
            });
        }
    }
}
