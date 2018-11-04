using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Internal;
using Rebus.Kafka.Tests.Factories;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.Kafka.Tests
{
    [TestFixture()]
    public class KafkaTestManyMessages : TestManyMessages<KafkaFactory>
    {
    }
}
