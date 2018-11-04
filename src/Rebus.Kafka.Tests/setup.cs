using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Docker.DotNet;
using Docker.DotNet.Models;
using NUnit.Framework;
using NUnit.Framework.Internal;

namespace Rebus.Kafka.Tests
{
    [SetUpFixture()]
    public class Setup
    {
#if NET46
        public static bool isWindows = true;
#else
        public static bool isWindows = System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
#endif
        private Uri LocalDockerUri()
        {
            return isWindows ? new Uri("npipe://./pipe/docker_engine") : new Uri("unix:///var/run/docker.sock");
        }

        [OneTimeSetUp]
        public async Task OneTimeSetup()
        {
            //docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=127.0.0.1 --env ADVERTISED_PORT=9092 --name kafka-rebus spotify/kafka
            TestContext.Out.WriteLine("Starting One time setup");
            var client = new DockerClientConfiguration(LocalDockerUri())
                .CreateClient();
            var listCont = await client.Containers.ListContainersAsync(new ContainersListParameters());
            TestContext.Out.WriteLine("I found {0} running containers on the local machine", listCont.Count);
            // the docker images are started, need to restart them
            foreach (var x in listCont.Where(x => x.Names.Any(y => y.Contains("kafka-rebus"))))
            {
                var i = await client.Containers.StopContainerAsync(x.ID, new ContainerStopParameters());
                await client.Containers.RemoveContainerAsync(x.ID, new ContainerRemoveParameters());
            }

            var parameters = new Docker.DotNet.Models.Config
            {
                Image = "spotify/kafka",
                ArgsEscaped = true,
                AttachStderr = true,
                AttachStdin = true,
                AttachStdout = true,
                Env = new List<string> { "ADVERTISED_PORT=9092", "ADVERTISED_HOST=127.0.0.1" }
            };

            var ports2181 = new List<PortBinding> {new PortBinding() {HostPort = "2181", HostIP = ""}};
            var ports9092 = new List<PortBinding> {new PortBinding() {HostPort = "9092", HostIP = ""}};

            var resp = await client.Containers.CreateContainerAsync(new CreateContainerParameters(parameters)
            {
                HostConfig = new HostConfig {PortBindings = new Dictionary<string, IList<PortBinding>>() {{ "2181/tcp", ports2181 },{ "9092/tcp", ports9092 } }},
                Name = "kafka-rebus"
            });
            await client.Containers.StartContainerAsync(resp.ID, new ContainerStartParameters());
            // it takes a few seconds for the agents to come online and register with the servers
            Thread.Sleep(5000);
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            TestContext.Out.WriteLine("Starting Teardown");
            DockerClient client = new DockerClientConfiguration(LocalDockerUri())
                .CreateClient();
            var listCont = await client.Containers.ListContainersAsync(new ContainersListParameters());
            TestContext.Out.WriteLine("I found {0} running containers on the local machine", listCont.Count);
            foreach (var x in listCont.Where(x => x.Names.Any(y => y.Contains("kafka-rebus"))))
            {
                TestContext.Out.WriteLine("Destorying {0}", string.Join(",", x.Names));
                var i = await client.Containers.StopContainerAsync(x.ID, new ContainerStopParameters());
                await client.Containers.RemoveContainerAsync(x.ID, new ContainerRemoveParameters());
            }
        }
    }
}
