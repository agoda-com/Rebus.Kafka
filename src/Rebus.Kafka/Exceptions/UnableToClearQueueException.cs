using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rebus.Messages;

namespace Rebus.Kafka.Exceptions
{
    public class UnableToClearQueueException : Exception
    {
        public TransportMessage[] remainingMessages;
    }
}
