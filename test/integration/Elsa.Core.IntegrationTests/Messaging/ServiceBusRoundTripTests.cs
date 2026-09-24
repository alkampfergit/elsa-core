using System;
using System.Threading.Tasks;
using Elsa.Services;
using Elsa.Testing.Shared.Unit;
using Microsoft.Extensions.DependencyInjection;
using Rebus.Handlers;
using Xunit;
using Xunit.Abstractions;

namespace Elsa.Core.IntegrationTests.Messaging
{
    /// <summary>
    /// Exercises Elsa's Rebus-based service bus end to end: <see cref="ICommandSender"/>/<see cref="IEventPublisher"/>
    /// through the real in-memory Rebus transport to an <see cref="IHandleMessages{TMessage}"/> consumer resolved
    /// from DI. This is meant to catch breakage from upgrading the Rebus / Rebus.ServiceProvider package versions,
    /// since nothing else in the test suite exercises the message bus.
    /// </summary>
    public class ServiceBusRoundTripTests : WorkflowsUnitTestBase
    {
        public ServiceBusRoundTripTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper,
                services => services
                    .AddSingleton<PingTracker>()
                    .AddTransient<IHandleMessages<PingCommand>>(sp => sp.GetRequiredService<PingTracker>())
                    .AddTransient<IHandleMessages<PongEvent>>(sp => sp.GetRequiredService<PingTracker>()),
                options => options
                    .AddCompetingMessageType<PingCommand>()
                    .AddPubSubMessageType<PongEvent>())
        {
        }

        [Fact(DisplayName = "ICommandSender.SendAsync round-trips through the real in-memory Rebus bus to a competing consumer.")]
        public async Task SendAsync_DeliversMessageToConsumer()
        {
            var commandSender = ServiceScope.ServiceProvider.GetRequiredService<ICommandSender>();
            var tracker = ServiceScope.ServiceProvider.GetRequiredService<PingTracker>();

            await commandSender.SendAsync(new PingCommand("hello"));

            var received = await tracker.PingReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal("hello", received);
        }

        [Fact(DisplayName = "IEventPublisher.PublishAsync round-trips through the real in-memory Rebus bus to a pub/sub consumer.")]
        public async Task PublishAsync_DeliversMessageToSubscriber()
        {
            var eventPublisher = ServiceScope.ServiceProvider.GetRequiredService<IEventPublisher>();
            var tracker = ServiceScope.ServiceProvider.GetRequiredService<PingTracker>();

            await eventPublisher.PublishAsync(new PongEvent("world"));

            var received = await tracker.PongReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal("world", received);
        }
    }

    public record PingCommand(string Value);

    public record PongEvent(string Value);

    public class PingTracker : IHandleMessages<PingCommand>, IHandleMessages<PongEvent>
    {
        public TaskCompletionSource<string> PingReceived { get; } = new();
        public TaskCompletionSource<string> PongReceived { get; } = new();

        public Task Handle(PingCommand message)
        {
            PingReceived.TrySetResult(message.Value);
            return Task.CompletedTask;
        }

        public Task Handle(PongEvent message)
        {
            PongReceived.TrySetResult(message.Value);
            return Task.CompletedTask;
        }
    }
}
