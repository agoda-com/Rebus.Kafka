﻿using System;
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
    [TestFixture()]
    public class KafkaBasicSendReceive : BasicSendReceive<KafkaTransportFactory>
    {
    }
}
