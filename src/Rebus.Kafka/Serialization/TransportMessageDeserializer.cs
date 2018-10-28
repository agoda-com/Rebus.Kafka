using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Rebus.Messages;

namespace Rebus.Kafka.Serialization
{
    class TransportMessageDeserializer : IDeserializer<TransportMessage>, IDisposable
    {
        public void Dispose()
        {
        }

        public TransportMessage Deserialize(string topic, byte[] data)
        {
            return JsonConvert.DeserializeObject<TransportMessage>(Encoding.UTF8.GetString(data));
        }

        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            return config;
        }
    }
}
