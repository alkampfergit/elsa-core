using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elsa.Activities.RabbitMq;
using Elsa.Activities.RabbitMq.Bookmarks;
using Elsa.Activities.RabbitMq.Configuration;
using Elsa.Activities.RabbitMq.Services;
using Elsa.Models;
using Elsa.Services;
using Elsa.Services.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Rebus.Messages;
using Xunit;

namespace Elsa.Core.IntegrationTests.RabbitMq
{
    /// <summary>
    /// Unit-level coverage of <see cref="Worker"/>, the glue between the RabbitMQ transport (<see cref="IClient"/>) and workflow dispatch.
    /// <see cref="IClient"/> is mocked, so no broker is needed: these tests verify that a worker subscribes exactly once, that a transport
    /// message handed to its handler is dispatched to <see cref="IWorkflowLaunchpad"/> as a <see cref="MessageReceivedBookmark"/> query built
    /// from the client's configuration (with the raw <see cref="TransportMessage"/> as workflow input, which is what
    /// <see cref="RabbitMqMessageReceived"/> expects), that every message is dispatched from its own disposed DI scope, and that disposing
    /// the worker hands the client back through the injected dispose delegate instead of disposing it itself.
    /// </summary>
    public class RabbitMqWorkerTests
    {
        private const string ConnectionString = "amqp://guest:guest@localhost:5672";
        private const string ExchangeName = "elsa-tests";
        private const string RoutingKey = "elsa.tests.worker";
        private const string TestHeaderName = "x-elsa-test";

        [Fact(DisplayName = "Constructing a Worker subscribes to the client exactly once with a message handler and dispatches nothing yet.")]
        public async Task Constructor_SubscribesToClientExactlyOnce()
        {
            await using var harness = new WorkerHarness();

            harness.Client.Verify(x => x.SubscribeWithHandler(It.IsAny<Func<TransportMessage, CancellationToken, Task>>()), Times.Once);
            Assert.NotNull(harness.Handler);
            Assert.Empty(harness.Dispatches);
            Assert.Equal(0, harness.ScopesCreated);
        }

        [Fact(DisplayName = "A message handed to the subscribed handler is dispatched as a MessageReceivedBookmark query with the transport message as workflow input.")]
        public async Task Handler_DispatchesReceivedMessageToWorkflowLaunchpad()
        {
            await using var harness = new WorkerHarness();
            var message = CreateTransportMessage("\"hello from rabbitmq\"");

            await harness.Handler!(message, CancellationToken.None);

            var dispatch = Assert.Single(harness.Dispatches);
            Assert.Equal(nameof(RabbitMqMessageReceived), dispatch.Query.ActivityType);

            // The bookmark must mirror the client's configuration, since that is what the bookmark indexer produced from the activity's inputs.
            var bookmark = Assert.IsType<MessageReceivedBookmark>(dispatch.Query.Bookmark);
            Assert.Equal(ExchangeName, bookmark.ExchangeName);
            Assert.Equal(RoutingKey, bookmark.RoutingKey);
            Assert.Equal(ConnectionString, bookmark.ConnectionString);
            Assert.Equal(harness.Configuration.Headers, bookmark.Headers);

            // A RabbitMQ message is not correlated to a particular workflow instance: every workflow waiting on this bookmark gets triggered.
            Assert.Null(dispatch.Query.CorrelationId);
            Assert.Null(dispatch.Query.WorkflowInstanceId);

            // RabbitMqMessageReceived casts the workflow input straight to TransportMessage, so the very same instance has to be passed along.
            Assert.NotNull(dispatch.Input);
            Assert.Same(message, dispatch.Input!.Input);
            Assert.Null(dispatch.Input.StorageProviderName);
        }

        [Fact(DisplayName = "Every received message is dispatched from its own DI scope, which is disposed afterwards.")]
        public async Task Handler_DispatchesEachMessageFromItsOwnDisposedScope()
        {
            await using var harness = new WorkerHarness();

            await harness.Handler!(CreateTransportMessage("\"first\""), CancellationToken.None);
            await harness.Handler!(CreateTransportMessage("\"second\""), CancellationToken.None);

            Assert.Equal(2, harness.Dispatches.Count);
            Assert.Equal(2, harness.ScopesCreated);
            Assert.Equal(2, harness.ScopesDisposed);
        }

        [Fact(DisplayName = "A handler invocation that is cancelled before the settling delay elapses never reaches workflow dispatch.")]
        public async Task Handler_DoesNotDispatchWhenCancelled()
        {
            await using var harness = new WorkerHarness();
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => harness.Handler!(CreateTransportMessage("\"cancelled\""), cancellation.Token));

            Assert.Empty(harness.Dispatches);
            Assert.Equal(0, harness.ScopesCreated);
        }

        [Fact(DisplayName = "Worker.DisposeAsync hands the subscribed client to the injected dispose delegate instead of disposing it directly.")]
        public async Task DisposeAsync_InvokesDisposeDelegateWithTheSubscribedClient()
        {
            await using var harness = new WorkerHarness();

            await harness.Worker.DisposeAsync();

            var disposedClient = Assert.Single(harness.DisposedClients);
            Assert.Same(harness.Client.Object, disposedClient);
            // The receiver factory owns the client's lifetime (BusClientFactory.DisposeReceiverAsync), so the worker must not dispose it itself.
            harness.Client.Verify(x => x.Dispose(), Times.Never);
        }

        private static TransportMessage CreateTransportMessage(string body) =>
            new(new Dictionary<string, string>
            {
                [Headers.MessageId] = Guid.NewGuid().ToString(),
                [TestHeaderName] = "worker"
            }, Encoding.UTF8.GetBytes(body));

        /// <summary>
        /// Builds a real <see cref="Worker"/> around a strict <see cref="IClient"/> mock (so any unexpected client call fails the test), a strict
        /// <see cref="IWorkflowLaunchpad"/> mock resolved through a genuine <see cref="ServiceProvider"/> scope, and a dispose delegate that
        /// records the client it is handed.
        /// </summary>
        private sealed class WorkerHarness : IAsyncDisposable
        {
            private readonly ServiceProvider _serviceProvider;
            private int _scopesCreated;
            private int _scopesDisposed;

            public WorkerHarness()
            {
                Configuration = new RabbitMqBusConfiguration(
                    ConnectionString,
                    ExchangeName,
                    RoutingKey,
                    new Dictionary<string, string> { [TestHeaderName] = "worker" },
                    clientId: "Elsa-Test-WORKER");

                Client = new Mock<IClient>(MockBehavior.Strict);
                Client.SetupGet(x => x.Configuration).Returns(Configuration);
                Client
                    .Setup(x => x.SubscribeWithHandler(It.IsAny<Func<TransportMessage, CancellationToken, Task>>()))
                    .Callback<Func<TransportMessage, CancellationToken, Task>>(handler => Handler = handler);

                Launchpad = new Mock<IWorkflowLaunchpad>(MockBehavior.Strict);
                Launchpad
                    .Setup(x => x.CollectAndDispatchWorkflowsAsync(It.IsAny<WorkflowsQuery>(), It.IsAny<WorkflowInput?>(), It.IsAny<CancellationToken>()))
                    .Callback<WorkflowsQuery, WorkflowInput?, CancellationToken>((query, input, _) => Dispatches.Add((query, input)))
                    .ReturnsAsync(Array.Empty<CollectedWorkflow>());

                _serviceProvider = new ServiceCollection()
                    .AddScoped(_ => new ScopeProbe(this))
                    // Resolving the launchpad pulls in the scope probe, so scope creation and disposal can be observed per dispatched message.
                    .AddScoped(sp =>
                    {
                        sp.GetRequiredService<ScopeProbe>();
                        return Launchpad.Object;
                    })
                    .BuildServiceProvider();

                Worker = new Worker(
                    _serviceProvider.GetRequiredService<IServiceScopeFactory>(),
                    Client.Object,
                    client =>
                    {
                        DisposedClients.Add(client);
                        return Task.CompletedTask;
                    },
                    NullLogger<Worker>.Instance);
            }

            public RabbitMqBusConfiguration Configuration { get; }
            public Mock<IClient> Client { get; }
            public Mock<IWorkflowLaunchpad> Launchpad { get; }
            public Worker Worker { get; }
            public Func<TransportMessage, CancellationToken, Task>? Handler { get; private set; }
            public List<(WorkflowsQuery Query, WorkflowInput? Input)> Dispatches { get; } = new();
            public List<IClient> DisposedClients { get; } = new();
            public int ScopesCreated => _scopesCreated;
            public int ScopesDisposed => _scopesDisposed;

            public ValueTask DisposeAsync() => _serviceProvider.DisposeAsync();

            private sealed class ScopeProbe : IDisposable
            {
                private readonly WorkerHarness _harness;

                public ScopeProbe(WorkerHarness harness)
                {
                    _harness = harness;
                    Interlocked.Increment(ref _harness._scopesCreated);
                }

                public void Dispose() => Interlocked.Increment(ref _harness._scopesDisposed);
            }
        }
    }
}
