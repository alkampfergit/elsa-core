using System;
using System.Threading.Tasks;
using Elsa.Builders;
using Elsa.Models;
using Elsa.Persistence;
using Elsa.Services;
using Elsa.Testing.Shared.Unit;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

namespace Elsa.Core.IntegrationTests.Messaging
{
    /// <summary>
    /// Drives the real production dispatch path over Rebus instead of a synthetic message: <see cref="IWorkflowDefinitionDispatcher"/> and
    /// <see cref="IWorkflowInstanceDispatcher"/> (both implemented by <c>QueuingWorkflowDispatcher</c>) send an
    /// <see cref="ExecuteWorkflowDefinitionRequest"/> / <see cref="ExecuteWorkflowInstanceRequest"/> through the in-memory transport, and the
    /// consumers that Rebus resolves from DI (with their scoped dependencies) must actually run the workflow and persist the finished instance.
    /// The workflow is resolved through the real <see cref="IWorkflowRegistry"/>, not a mock.
    /// </summary>
    public class ServiceBusWorkflowDispatchTests : WorkflowsUnitTestBase
    {
        public ServiceBusWorkflowDispatchTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper,
                services => services.AddSingleton<DispatchedWorkflowTracker>(),
                options => options.AddWorkflow<DispatchedWorkflow>())
        {
        }

        [Fact(DisplayName = "IWorkflowDefinitionDispatcher runs a published workflow through Rebus and delivers the workflow input.")]
        public async Task DispatchAsync_ExecuteWorkflowDefinitionRequest_RunsWorkflowThroughRebus()
        {
            var dispatcher = ServiceScope.ServiceProvider.GetRequiredService<IWorkflowDefinitionDispatcher>();
            var tracker = ServiceScope.ServiceProvider.GetRequiredService<DispatchedWorkflowTracker>();

            await dispatcher.DispatchAsync(new ExecuteWorkflowDefinitionRequest(nameof(DispatchedWorkflow), Input: new WorkflowInput("definition-input")));

            var execution = await tracker.Executed.Task.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Equal("definition-input", execution.Input);

            var workflowInstance = await WaitForFinishedInstanceAsync(execution.WorkflowInstanceId);
            Assert.Equal(nameof(DispatchedWorkflow), workflowInstance.DefinitionId);
        }

        [Fact(DisplayName = "IWorkflowInstanceDispatcher executes a persisted idle workflow instance through Rebus and delivers the workflow input.")]
        public async Task DispatchAsync_ExecuteWorkflowInstanceRequest_RunsPersistedInstanceThroughRebus()
        {
            var dispatcher = ServiceScope.ServiceProvider.GetRequiredService<IWorkflowInstanceDispatcher>();
            var workflowFactory = ServiceScope.ServiceProvider.GetRequiredService<IWorkflowFactory>();
            var workflowInstanceStore = ServiceScope.ServiceProvider.GetRequiredService<IWorkflowInstanceStore>();
            var tracker = ServiceScope.ServiceProvider.GetRequiredService<DispatchedWorkflowTracker>();

            var workflowBlueprint = await WorkflowRegistry.FindAsync(nameof(DispatchedWorkflow), VersionOptions.Published);
            Assert.NotNull(workflowBlueprint);
            var workflowInstance = await workflowFactory.InstantiateAsync(workflowBlueprint!);
            await workflowInstanceStore.SaveAsync(workflowInstance);
            Assert.Equal(WorkflowStatus.Idle, workflowInstance.WorkflowStatus);

            await dispatcher.DispatchAsync(new ExecuteWorkflowInstanceRequest(workflowInstance.Id, Input: new WorkflowInput("instance-input")));

            var execution = await tracker.Executed.Task.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Equal(workflowInstance.Id, execution.WorkflowInstanceId);
            Assert.Equal("instance-input", execution.Input);

            var finishedInstance = await WaitForFinishedInstanceAsync(workflowInstance.Id);
            Assert.Equal(WorkflowStatus.Finished, finishedInstance.WorkflowStatus);
        }

        /// <summary>
        /// The consumer persists the instance on its own Rebus worker thread after the tracked activity ran, so poll briefly for the Finished status.
        /// </summary>
        private async Task<WorkflowInstance> WaitForFinishedInstanceAsync(string workflowInstanceId)
        {
            var workflowInstanceStore = ServiceScope.ServiceProvider.GetRequiredService<IWorkflowInstanceStore>();
            var deadline = DateTime.UtcNow.AddSeconds(10);

            while (true)
            {
                var workflowInstance = await workflowInstanceStore.FindByIdAsync(workflowInstanceId);

                if (workflowInstance?.WorkflowStatus == WorkflowStatus.Finished)
                    return workflowInstance;

                if (DateTime.UtcNow >= deadline)
                    throw new TimeoutException($"Workflow instance {workflowInstanceId} did not reach the Finished status in time (current status: {workflowInstance?.WorkflowStatus.ToString() ?? "not found"}).");

                await Task.Delay(100);
            }
        }
    }

    public record DispatchedWorkflowExecution(string WorkflowInstanceId, string? Input);

    public class DispatchedWorkflowTracker
    {
        public TaskCompletionSource<DispatchedWorkflowExecution> Executed { get; } = new();
    }

    /// <summary>
    /// A single-activity workflow that reports the instance it ran in and the workflow input it received.
    /// </summary>
    public class DispatchedWorkflow : IWorkflow
    {
        private readonly DispatchedWorkflowTracker _tracker;

        public DispatchedWorkflow(DispatchedWorkflowTracker tracker) => _tracker = tracker;

        public void Build(IWorkflowBuilder builder)
        {
            builder.Then(context => { _tracker.Executed.TrySetResult(new DispatchedWorkflowExecution(context.WorkflowInstance.Id, context.GetInput<string>())); });
        }
    }
}
