using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elsa.Services;
using Elsa.Testing.Shared.Unit;
using Microsoft.Extensions.DependencyInjection;
using NodaTime;
using Rebus.Handlers;
using Rebus.Messages;
using Rebus.Pipeline;
using Xunit;
using Xunit.Abstractions;

namespace Elsa.Core.IntegrationTests.Messaging
{
    /// <summary>
    /// Complements <see cref="ServiceBusRoundTripTests"/> with the delivery semantics Elsa relies on beyond a single send/publish:
    /// deferred delivery, several message types sharing one competing-consumer queue (as the production "ExecuteWorkflow" queue does),
    /// pub/sub fan-out to every subscribed endpoint, several handlers for one event, header propagation and Newtonsoft.Json payload
    /// round-tripping. Everything runs on the real in-memory Rebus transport, so each test guards a different way in which a Rebus
    /// package upgrade could break Elsa's messaging.
    /// </summary>
    public class ServiceBusDeliveryScenarioTests : WorkflowsUnitTestBase
    {
        private const string SharedQueueName = "SharedCommandQueue";
        private const string CustomHeaderName = "elsa-test-header";

        public ServiceBusDeliveryScenarioTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper,
                services => services
                    .AddSingleton<ScenarioTracker>()
                    .AddTransient<IHandleMessages<AlphaCommand>>(sp => sp.GetRequiredService<ScenarioTracker>())
                    .AddTransient<IHandleMessages<BetaCommand>>(sp => sp.GetRequiredService<ScenarioTracker>())
                    .AddTransient<IHandleMessages<DeferredCommand>>(sp => sp.GetRequiredService<ScenarioTracker>())
                    .AddTransient<IHandleMessages<HeaderCommand>>(sp => sp.GetRequiredService<ScenarioTracker>())
                    .AddTransient<IHandleMessages<RichPayloadCommand>>(sp => sp.GetRequiredService<ScenarioTracker>())
                    .AddTransient<IHandleMessages<FanOutEvent>>(sp => sp.GetRequiredService<ScenarioTracker>())
                    .AddTransient<IHandleMessages<MultiHandlerEvent>, FirstMultiHandlerEventHandler>()
                    .AddTransient<IHandleMessages<MultiHandlerEvent>, SecondMultiHandlerEventHandler>(),
                options => options
                    // Two distinct message types on one queue, mirroring how the production ExecuteWorkflow queue is shared.
                    .AddCompetingMessageType<AlphaCommand>(SharedQueueName)
                    .AddCompetingMessageType<BetaCommand>(SharedQueueName)
                    .AddCompetingMessageType<DeferredCommand>()
                    .AddCompetingMessageType<HeaderCommand>()
                    .AddCompetingMessageType<RichPayloadCommand>()
                    // The same event registered on two queues yields two subscribing endpoints, so a single publish must fan out to both.
                    .AddPubSubMessageType<FanOutEvent>("FanOutSubscriberA")
                    .AddPubSubMessageType<FanOutEvent>("FanOutSubscriberB")
                    .AddPubSubMessageType<MultiHandlerEvent>())
        {
        }

        [Fact(DisplayName = "ICommandSender.DeferAsync delivers the message after the requested delay through the Rebus timeout manager.")]
        public async Task DeferAsync_DeliversMessageAfterDelay()
        {
            var commandSender = ServiceScope.ServiceProvider.GetRequiredService<ICommandSender>();
            var tracker = ServiceScope.ServiceProvider.GetRequiredService<ScenarioTracker>();
            var delay = Duration.FromSeconds(2);
            var sentAt = DateTimeOffset.UtcNow;

            await commandSender.DeferAsync(new DeferredCommand("later"), delay);

            var received = await tracker.DeferredReceived.Task.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Equal("later", received.Value);

            // Only a lower bound is asserted so the test is not coupled to scheduler timing; a message that arrives (almost) immediately was not deferred at all.
            var elapsed = received.ReceivedAt - sentAt;
            var headerDump = string.Join(", ", received.Headers.Select(header => $"{header.Key}={header.Value}"));
            Assert.True(elapsed >= delay.ToTimeSpan() / 2, $"Deferred message arrived after {elapsed}, which is too early for a {delay} delay. Received headers: {headerDump}");
        }

        [Fact(DisplayName = "Two message types registered on one competing-consumer queue share a single bus and are each routed to their own handler.")]
        public async Task SendAsync_MessageTypesSharingAQueue_AreRoutedToTheirOwnHandlers()
        {
            var serviceBusFactory = ServiceScope.ServiceProvider.GetRequiredService<IServiceBusFactory>();
            var commandSender = ServiceScope.ServiceProvider.GetRequiredService<ICommandSender>();
            var tracker = ServiceScope.ServiceProvider.GetRequiredService<ScenarioTracker>();

            // Precondition: both types resolve to the same bus, i.e. they genuinely share one queue.
            var alphaBus = await serviceBusFactory.GetServiceBusAsync(typeof(AlphaCommand));
            var betaBus = await serviceBusFactory.GetServiceBusAsync(typeof(BetaCommand));
            Assert.Same(alphaBus, betaBus);

            await commandSender.SendAsync(new AlphaCommand("alpha"));
            await commandSender.SendAsync(new BetaCommand("beta"));

            var alpha = await tracker.AlphaReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var beta = await tracker.BetaReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal("alpha", alpha.Value);
            Assert.Equal("beta", beta.Value);

            // Give a mis-routed or duplicated delivery a chance to show up before checking each handler ran exactly once.
            await Task.Delay(500);
            Assert.Equal(1, tracker.AlphaHandled);
            Assert.Equal(1, tracker.BetaHandled);
        }

        [Fact(DisplayName = "IEventPublisher.PublishAsync fans a single event out to every endpoint subscribed through the subscription storage.")]
        public async Task PublishAsync_FansOutToEverySubscribedEndpoint()
        {
            var eventPublisher = ServiceScope.ServiceProvider.GetRequiredService<IEventPublisher>();
            var tracker = ServiceScope.ServiceProvider.GetRequiredService<ScenarioTracker>();

            await eventPublisher.PublishAsync(new FanOutEvent("fan-out"));

            // Two endpoints subscribed to FanOutEvent, so the handler must run once per endpoint for a single publish.
            await tracker.FanOutDeliveredTwice.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.All(tracker.FanOutValues, value => Assert.Equal("fan-out", value));

            // Give a duplicate delivery a chance to show up before checking there were exactly two deliveries.
            await Task.Delay(500);
            Assert.Equal(2, tracker.FanOutValues.Count);
        }

        [Fact(DisplayName = "Every IHandleMessages<T> registered in DI for an event type is invoked for a published event.")]
        public async Task PublishAsync_InvokesEveryHandlerRegisteredForTheEvent()
        {
            var eventPublisher = ServiceScope.ServiceProvider.GetRequiredService<IEventPublisher>();
            var tracker = ServiceScope.ServiceProvider.GetRequiredService<ScenarioTracker>();

            await eventPublisher.PublishAsync(new MultiHandlerEvent("multi"));

            var first = await tracker.FirstMultiHandlerReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var second = await tracker.SecondMultiHandlerReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal("multi", first);
            Assert.Equal("multi", second);
        }

        [Fact(DisplayName = "Custom headers passed to ICommandSender.SendAsync reach the handler through the Rebus message context.")]
        public async Task SendAsync_PreservesCustomHeaders()
        {
            var commandSender = ServiceScope.ServiceProvider.GetRequiredService<ICommandSender>();
            var tracker = ServiceScope.ServiceProvider.GetRequiredService<ScenarioTracker>();
            var headers = new Dictionary<string, string> { [CustomHeaderName] = "header-value" };

            await commandSender.SendAsync(new HeaderCommand("with-headers"), headers: headers);

            var received = await tracker.HeaderCommandReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal("header-value", received[CustomHeaderName]);
            Assert.True(received.ContainsKey(Headers.MessageId), $"Expected the {Headers.MessageId} header to be present.");
            Assert.Contains(typeof(HeaderCommand).FullName!, received[Headers.Type]);
        }

        [Fact(DisplayName = "A non-trivial payload round-trips through Rebus' Newtonsoft.Json serializer configured with Elsa's default settings.")]
        public async Task SendAsync_RoundTripsRichPayloadThroughNewtonsoftJson()
        {
            var commandSender = ServiceScope.ServiceProvider.GetRequiredService<ICommandSender>();
            var tracker = ServiceScope.ServiceProvider.GetRequiredService<ScenarioTracker>();
            var sent = new RichPayloadCommand(
                Instant.FromUtc(2024, 2, 29, 13, 45, 30),
                Duration.FromMinutes(90),
                new List<string> { "one", "two" },
                new Dictionary<string, string> { ["Key"] = "value", ["camelCase"] = "kept" },
                new NestedPayload("nested", 42),
                new PolymorphicPayload("polymorphic"));

            await commandSender.SendAsync(sent);

            var received = await tracker.RichPayloadReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.NotSame(sent, received);
            Assert.Equal(sent.When, received.When); // NodaTime types need Elsa's NodaTime converters to be applied to the Rebus serializer.
            Assert.Equal(sent.HowLong, received.HowLong);
            Assert.Equal(sent.Tags, received.Tags);
            Assert.Equal("value", received.Attributes["Key"]); // Elsa's naming strategy leaves dictionary keys untouched.
            Assert.Equal("kept", received.Attributes["camelCase"]);
            Assert.Equal(sent.Child, received.Child);
            var polymorphic = Assert.IsType<PolymorphicPayload>(received.Polymorphic); // Requires TypeNameHandling.Auto to survive the round-trip.
            Assert.Equal("polymorphic", polymorphic.Name);
        }
    }

    public record AlphaCommand(string Value);

    public record BetaCommand(string Value);

    public record DeferredCommand(string Value);

    public record HeaderCommand(string Value);

    public record FanOutEvent(string Value);

    public record MultiHandlerEvent(string Value);

    public record NestedPayload(string Name, int Number);

    public record PolymorphicPayload(string Name);

    public record RichPayloadCommand(Instant When, Duration HowLong, IList<string> Tags, IDictionary<string, string> Attributes, NestedPayload Child, object Polymorphic);

    public record DeferredDelivery(string Value, DateTimeOffset ReceivedAt, IDictionary<string, string> Headers);

    public class ScenarioTracker :
        IHandleMessages<AlphaCommand>,
        IHandleMessages<BetaCommand>,
        IHandleMessages<DeferredCommand>,
        IHandleMessages<HeaderCommand>,
        IHandleMessages<RichPayloadCommand>,
        IHandleMessages<FanOutEvent>
    {
        private int _alphaHandled;
        private int _betaHandled;

        public TaskCompletionSource<AlphaCommand> AlphaReceived { get; } = new();
        public TaskCompletionSource<BetaCommand> BetaReceived { get; } = new();
        public TaskCompletionSource<DeferredDelivery> DeferredReceived { get; } = new();
        public TaskCompletionSource<IDictionary<string, string>> HeaderCommandReceived { get; } = new();
        public TaskCompletionSource<RichPayloadCommand> RichPayloadReceived { get; } = new();
        public TaskCompletionSource<bool> FanOutDeliveredTwice { get; } = new();
        public TaskCompletionSource<string> FirstMultiHandlerReceived { get; } = new();
        public TaskCompletionSource<string> SecondMultiHandlerReceived { get; } = new();
        public ConcurrentQueue<string> FanOutValues { get; } = new();
        public int AlphaHandled => _alphaHandled;
        public int BetaHandled => _betaHandled;

        public Task Handle(AlphaCommand message)
        {
            Interlocked.Increment(ref _alphaHandled);
            AlphaReceived.TrySetResult(message);
            return Task.CompletedTask;
        }

        public Task Handle(BetaCommand message)
        {
            Interlocked.Increment(ref _betaHandled);
            BetaReceived.TrySetResult(message);
            return Task.CompletedTask;
        }

        public Task Handle(DeferredCommand message)
        {
            var headers = MessageContext.Current?.Headers ?? new Dictionary<string, string>();
            DeferredReceived.TrySetResult(new DeferredDelivery(message.Value, DateTimeOffset.UtcNow, new Dictionary<string, string>(headers)));
            return Task.CompletedTask;
        }

        public Task Handle(HeaderCommand message)
        {
            var messageContext = MessageContext.Current;

            if (messageContext == null)
                HeaderCommandReceived.TrySetException(new InvalidOperationException("No ambient Rebus message context was available in the handler."));
            else
                HeaderCommandReceived.TrySetResult(new Dictionary<string, string>(messageContext.Headers));

            return Task.CompletedTask;
        }

        public Task Handle(RichPayloadCommand message)
        {
            RichPayloadReceived.TrySetResult(message);
            return Task.CompletedTask;
        }

        public Task Handle(FanOutEvent message)
        {
            FanOutValues.Enqueue(message.Value);

            if (FanOutValues.Count >= 2)
                FanOutDeliveredTwice.TrySetResult(true);

            return Task.CompletedTask;
        }
    }

    public class FirstMultiHandlerEventHandler : IHandleMessages<MultiHandlerEvent>
    {
        private readonly ScenarioTracker _tracker;
        public FirstMultiHandlerEventHandler(ScenarioTracker tracker) => _tracker = tracker;

        public Task Handle(MultiHandlerEvent message)
        {
            _tracker.FirstMultiHandlerReceived.TrySetResult(message.Value);
            return Task.CompletedTask;
        }
    }

    public class SecondMultiHandlerEventHandler : IHandleMessages<MultiHandlerEvent>
    {
        private readonly ScenarioTracker _tracker;
        public SecondMultiHandlerEventHandler(ScenarioTracker tracker) => _tracker = tracker;

        public Task Handle(MultiHandlerEvent message)
        {
            _tracker.SecondMultiHandlerReceived.TrySetResult(message.Value);
            return Task.CompletedTask;
        }
    }
}
