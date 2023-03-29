using Xunit;
using System.Threading.Tasks;
using Elsa.Core.IntegrationTests.Autofixture;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using System.Threading;
using Elsa.Services;
using Elsa.Core.IntegrationTests.Workflows;
using Elsa.Persistence;
using Elsa.Testing.Shared;
using Elsa.Testing.Shared.Helpers;
using Elsa.Providers.WorkflowStorage;
using Storage.Net;
using Elsa.Providers.Workflows;

namespace Elsa.Core.IntegrationTests.Persistence.MongoDb
{
    public class MongoDbIntegrationTests
    {
        [Theory(DisplayName = "A persistable workflow instance with default persistence behaviour should be persisted-to and readable-from a MongoDb store after being run"), AutoMoqData]
        public async Task APersistableWorkflowInstanceWithDefaultPersistenceBehaviourShouldBeRoundTrippable([WithPersistableWorkflow,WithMongoDb] ElsaHostBuilderBuilder hostBuilderBuilder)
        {
            var hostBuilder = hostBuilderBuilder.GetHostBuilder();
            hostBuilder.ConfigureServices((ctx, services) => services.AddHostedService<HostedWorkflowRunner>());
            var host = await hostBuilder.StartAsync();
        }

        class HostedWorkflowRunner : IHostedService
        {
            readonly IBuildsAndStartsWorkflow _workflowRunner;
            readonly IWorkflowInstanceStore _instanceStore;

            public async Task StartAsync(CancellationToken cancellationToken)
            {
                var runWorkflowResult = await _workflowRunner.BuildAndStartWorkflowAsync<PersistableWorkflow>();
                var instance = runWorkflowResult.WorkflowInstance!;
                var retrievedInstance = await _instanceStore.FindByIdAsync(instance.Id);

                Assert.NotNull(retrievedInstance);
            }

            public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            public HostedWorkflowRunner(IBuildsAndStartsWorkflow workflowRunner, IWorkflowInstanceStore instanceStore)
            {
                _workflowRunner = workflowRunner ?? throw new System.ArgumentNullException(nameof(workflowRunner));
                _instanceStore = instanceStore ?? throw new System.ArgumentNullException(nameof(instanceStore));
            }
        }

        [Theory(DisplayName = "A persistable workflow instance with default persistence behaviour should be persisted-to and readable-from a MongoDb store after being run"), AutoMoqData]
        public async Task APersistableWorkflowInstanceReallyBigMustBePersisted([WithPersistableWorkflow, WithMongoDb] ElsaHostBuilderBuilder hostBuilderBuilder)
        {
            var hostBuilder = hostBuilderBuilder.GetHostBuilder();
            hostBuilderBuilder.ElsaCallbacks.Add(elsa =>
            {
                elsa.UseDefaultWorkflowStorageProvider<BlobStorageWorkflowStorageProvider>();
            });
            hostBuilder.ConfigureServices((ctx, services) =>
            {
                services.AddHostedService<HostedBigWorkflowRunner>();
                services.Configure<BlobStorageWorkflowStorageProviderOptions>(options =>
                {
                    options.BlobStorageFactory = () => StorageFactory.Blobs.DirectoryFiles("c:\\temp\\elsatest");
                });

                services.Configure<BlobStorageWorkflowProviderOptions>(options =>
                {
                    options.BlobStorageFactory = () => StorageFactory.Blobs.DirectoryFiles("c:\\temp\\elsatest");
                });

                //services.AddElsa(elsa =>
                //{
                //    elsa.UseDefaultWorkflowStorageProvider<BlobStorageWorkflowStorageProvider>();
                //});
            });

            var host = await hostBuilder.StartAsync();
        }

        class HostedBigWorkflowRunner : IHostedService
        {
            readonly IBuildsAndStartsWorkflow _workflowRunner;
            readonly IWorkflowInstanceStore _instanceStore;

            public async Task StartAsync(CancellationToken cancellationToken)
            {
                var runWorkflowResult = await _workflowRunner.BuildAndStartWorkflowAsync<PersistableBigWorkflow>();
                var instance = runWorkflowResult.WorkflowInstance!;
                var retrievedInstance = await _instanceStore.FindByIdAsync(instance.Id);

                Assert.NotNull(retrievedInstance);
            }

            public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            public HostedBigWorkflowRunner(IBuildsAndStartsWorkflow workflowRunner, IWorkflowInstanceStore instanceStore)
            {
                _workflowRunner = workflowRunner ?? throw new System.ArgumentNullException(nameof(workflowRunner));
                _instanceStore = instanceStore ?? throw new System.ArgumentNullException(nameof(instanceStore));
            }
        }
    }
}