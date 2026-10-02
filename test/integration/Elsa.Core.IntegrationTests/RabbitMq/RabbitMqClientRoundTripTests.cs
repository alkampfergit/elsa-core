using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elsa.Activities.RabbitMq.Configuration;
using Elsa.Activities.RabbitMq.Helpers;
using Elsa.Activities.RabbitMq.Services;
using RabbitMQ.Client;
using Rebus.Messages;
using Xunit;
using Xunit.Abstractions;
using RebusHeaders = Rebus.Messages.Headers;

namespace Elsa.Core.IntegrationTests.RabbitMq
{
    /// <summary>
    /// Exercises <see cref="Client"/> against a real RabbitMQ broker, i.e. the Rebus.RabbitMq transport itself (<c>UseRabbitMq</c> for the
    /// subscriber, <c>UseRabbitMqAsOneWayClient</c> for the publisher, topic subscribe/publish and transport-message forwarding) rather than
    /// Rebus' in-memory transport that the other messaging tests use. This is the only place the Rebus.RabbitMq / RabbitMQ.Client package
    /// versions are actually put on the wire.
    /// <para>
    /// The broker is expected at <c>amqp://guest:guest@localhost:5672</c>, which is what the repository's <c>docker-compose.yaml</c>
    /// <c>rabbitmq</c> service exposes (<c>docker-compose up rabbitmq</c>); set the <c>ELSA_RABBITMQ_CONNECTION_STRING</c> environment
    /// variable to point somewhere else. Like <see cref="Workflows.ServiceBusWorkflowTest"/>, each test disables itself (logs and returns)
    /// when no broker is reachable, so the suite stays green on machines without Docker while the tests run wherever the broker is up.
    /// </para>
    /// </summary>
    public class RabbitMqClientRoundTripTests
    {
        private const string DefaultConnectionString = "amqp://guest:guest@localhost:5672";
        private const string ConnectionStringVariable = "ELSA_RABBITMQ_CONNECTION_STRING";
        private const int DefaultAmqpPort = 5672;
        private const string TestHeaderName = "x-elsa-test";

        private static readonly TimeSpan ProbeTimeout = TimeSpan.FromMilliseconds(250);
        private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(15);

        // Client.SubscribeWithHandler starts the topic subscription without awaiting it, so the queue binding needs a moment before publishing.
        private static readonly TimeSpan SubscriptionSettleTime = TimeSpan.FromSeconds(1);

        // How long to wait for a delivery that must NOT happen before concluding it did not.
        private static readonly TimeSpan NoDeliveryGracePeriod = TimeSpan.FromMilliseconds(500);

        private readonly ITestOutputHelper _testOutputHelper;
        private readonly string _connectionString;

        public RabbitMqClientRoundTripTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
            var configuredConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
            _connectionString = string.IsNullOrWhiteSpace(configuredConnectionString) ? DefaultConnectionString : configuredConnectionString;
        }

        [Fact(DisplayName = "Client.PublishMessage reaches a Client subscribed with SubscribeWithHandler through a real RabbitMQ broker.")]
        public async Task PublishMessage_IsReceivedBySubscribedClient()
        {
            if (!await IsBrokerAvailableAsync())
                return;

            var routingKey = UniqueName("elsa.tests.roundtrip");
            var headers = new Dictionary<string, string> { [TestHeaderName] = "round-trip" };
            var subscriber = new Client(CreateConfiguration(exchangeName: "", routingKey, headers));
            var publisher = new Client(CreateConfiguration(exchangeName: "", routingKey, headers));

            try
            {
                var received = new TaskCompletionSource<TransportMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

                subscriber.SubscribeWithHandler((message, _) =>
                {
                    received.TrySetResult(message);
                    return Task.CompletedTask;
                });
                await Task.Delay(SubscriptionSettleTime);

                var payload = $"hello from elsa {Guid.NewGuid():N}";
                await publisher.PublishMessage(payload);

                var transportMessage = await received.Task.WaitAsync(ReceiveTimeout);

                // Rebus serializes the published string as JSON (i.e. quoted), and that raw body is what RabbitMqMessageReceived surfaces as its Output.
                var body = Encoding.UTF8.GetString(transportMessage.Body);
                Assert.Contains(payload, body);
                Assert.Equal("round-trip", transportMessage.Headers[TestHeaderName]);
                Assert.True(transportMessage.Headers.ContainsKey(RebusHeaders.MessageId), $"Expected the {RebusHeaders.MessageId} header to be present. Received headers: {DumpHeaders(transportMessage)}");
            }
            finally
            {
                DisposeClients(subscriber, publisher);
            }
        }

        [Fact(DisplayName = "A subscribed Client only hands messages whose headers match its configured headers to the handler.")]
        public async Task SubscribeWithHandler_IgnoresMessagesWhoseHeadersDoNotMatch()
        {
            if (!await IsBrokerAvailableAsync())
                return;

            var routingKey = UniqueName("elsa.tests.headers");
            var subscriber = new Client(CreateConfiguration(exchangeName: "", routingKey, new Dictionary<string, string> { [TestHeaderName] = "expected" }));
            var mismatchingPublisher = new Client(CreateConfiguration(exchangeName: "", routingKey, new Dictionary<string, string> { [TestHeaderName] = "other" }));
            var matchingPublisher = new Client(CreateConfiguration(exchangeName: "", routingKey, new Dictionary<string, string> { [TestHeaderName] = "expected" }));

            try
            {
                var deliveries = new ConcurrentQueue<TransportMessage>();
                var firstDelivery = new TaskCompletionSource<TransportMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

                subscriber.SubscribeWithHandler((message, _) =>
                {
                    deliveries.Enqueue(message);
                    firstDelivery.TrySetResult(message);
                    return Task.CompletedTask;
                });
                await Task.Delay(SubscriptionSettleTime);

                // Both publishers hit the same topic, so both messages land in the subscriber's queue; only the matching one may reach the handler.
                // (The mismatching one falls through to Rebus' regular pipeline, which has no handler for it and eventually dead-letters it.)
                await mismatchingPublisher.PublishMessage("must not be handled");
                await matchingPublisher.PublishMessage("must be handled");

                var delivered = await firstDelivery.Task.WaitAsync(ReceiveTimeout);
                Assert.Equal("expected", delivered.Headers[TestHeaderName]);
                Assert.Contains("must be handled", Encoding.UTF8.GetString(delivered.Body));

                await Task.Delay(NoDeliveryGracePeriod);
                var onlyDelivery = Assert.Single(deliveries);
                Assert.Same(delivered, onlyDelivery);
            }
            finally
            {
                DisposeClients(subscriber, mismatchingPublisher, matchingPublisher);
            }
        }

        [Fact(DisplayName = "Client round-trips a message through a named topic exchange, the routingKey@exchange form the RabbitMQ activities use.")]
        public async Task PublishMessage_IsReceivedThroughNamedExchange()
        {
            if (!await IsBrokerAvailableAsync())
                return;

            var exchangeName = UniqueName("elsa-tests");
            var routingKey = UniqueName("elsa.tests.exchange");
            var headers = new Dictionary<string, string> { [TestHeaderName] = "named-exchange" };

            // Rebus.RabbitMq only declares its own RebusDirect/RebusTopics exchanges. An exchange named through the activities' ExchangeName input
            // has to exist on the broker already (as it would in production), otherwise binding the subscriber's queue to it fails.
            await DeclareTopicExchangeAsync(exchangeName);

            var subscriber = new Client(CreateConfiguration(exchangeName, routingKey, headers));
            var publisher = new Client(CreateConfiguration(exchangeName, routingKey, headers));

            try
            {
                Assert.Equal($"{routingKey}@{exchangeName}", subscriber.Configuration.TopicFullName);

                var received = new TaskCompletionSource<TransportMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

                subscriber.SubscribeWithHandler((message, _) =>
                {
                    received.TrySetResult(message);
                    return Task.CompletedTask;
                });
                await Task.Delay(SubscriptionSettleTime);

                var payload = $"via named exchange {Guid.NewGuid():N}";
                await publisher.PublishMessage(payload);

                var transportMessage = await received.Task.WaitAsync(ReceiveTimeout);
                Assert.Contains(payload, Encoding.UTF8.GetString(transportMessage.Body));
                Assert.Equal("named-exchange", transportMessage.Headers[TestHeaderName]);
            }
            finally
            {
                DisposeClients(subscriber, publisher);
                await DeleteExchangeAsync(exchangeName);
            }
        }

        /// <summary>
        /// Probes the broker with a short TCP connect so the tests run wherever the docker-compose broker is up and disable themselves
        /// (following the convention of <see cref="Workflows.ServiceBusWorkflowTest"/>) everywhere else.
        /// </summary>
        private async Task<bool> IsBrokerAvailableAsync()
        {
            var uri = new Uri(_connectionString);
            var port = uri.Port > 0 ? uri.Port : DefaultAmqpPort;

            try
            {
                using var tcpClient = new TcpClient();
                using var timeout = new CancellationTokenSource(ProbeTimeout);
                await tcpClient.ConnectAsync(uri.Host, port, timeout.Token);

                if (tcpClient.Connected)
                    return true;
            }
            catch (Exception e) when (e is SocketException or OperationCanceledException)
            {
            }

            _testOutputHelper.WriteLine(
                $"RabbitMQ integration test is disabled: no broker reachable at {uri.Host}:{port} within {ProbeTimeout.TotalMilliseconds} ms. " +
                $"Run 'docker-compose up rabbitmq' from the repository root (or set {ConnectionStringVariable}) to enable.");
            return false;
        }

        private RabbitMqBusConfiguration CreateConfiguration(string exchangeName, string routingKey, Dictionary<string, string> headers) =>
            new(
                _connectionString,
                exchangeName,
                routingKey,
                headers,
                RabbitMqClientConfigurationHelper.GetTestClientId(Guid.NewGuid().ToString("N")),
                autoDeleteQueue: true);

        private async Task DeclareTopicExchangeAsync(string exchangeName)
        {
            var connectionFactory = new ConnectionFactory { Uri = new Uri(_connectionString) };
            await using var connection = await connectionFactory.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();
            await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Topic, durable: false, autoDelete: false);
        }

        private async Task DeleteExchangeAsync(string exchangeName)
        {
            try
            {
                var connectionFactory = new ConnectionFactory { Uri = new Uri(_connectionString) };
                await using var connection = await connectionFactory.CreateConnectionAsync();
                await using var channel = await connection.CreateChannelAsync();
                await channel.ExchangeDeleteAsync(exchangeName);
            }
            catch (Exception e)
            {
                _testOutputHelper.WriteLine($"Could not delete test exchange '{exchangeName}': {e.Message}");
            }
        }

        private void DisposeClients(params Client[] clients)
        {
            foreach (var client in clients)
            {
                try
                {
                    client.Dispose();
                }
                catch (Exception e)
                {
                    _testOutputHelper.WriteLine($"Disposing a test client failed: {e.Message}");
                }
            }
        }

        private static string UniqueName(string prefix) => $"{prefix}.{Guid.NewGuid():N}";

        private static string DumpHeaders(TransportMessage message) => string.Join(", ", message.Headers.Select(header => $"{header.Key}={header.Value}"));
    }
}
