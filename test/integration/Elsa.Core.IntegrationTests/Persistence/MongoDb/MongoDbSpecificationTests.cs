using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Elsa.Activities.Webhooks;
using Elsa.Models;
using Elsa.Persistence;
using Elsa.Persistence.MongoDb;
using Elsa.Persistence.MongoDb.Stores;
using Elsa.Persistence.Specifications;
using Elsa.Persistence.Specifications.WorkflowDefinitions;
using Elsa.Persistence.Specifications.WorkflowInstances;
using Elsa.Retention.Specifications;
using Elsa.Secrets;
using Elsa.Secrets.Models;
using Elsa.Secrets.Persistence.MongoDb.Extensions;
using Elsa.Secrets.Persistence.MongoDb.Stores;
using Elsa.Secrets.Persistence.Specifications;
using Elsa.Secrets.Specifications;
using Elsa.Webhooks.Models;
using Elsa.Webhooks.Persistence.MongoDb.Extensions;
using Elsa.Webhooks.Persistence.MongoDb.Stores;
using Elsa.WorkflowSettings;
using Elsa.WorkflowSettings.Models;
using Elsa.WorkflowSettings.Persistence.MongoDb.Extensions;
using Elsa.WorkflowSettings.Persistence.MongoDb.Stores;
using Elsa.WorkflowSettings.Persistence.Specification.WorkflowSettingsDefinitions;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using NodaTime;
using Xunit;
using Xunit.Abstractions;
using BookmarkSpecs = Elsa.Persistence.Specifications.Bookmarks;
using SortDirection = Elsa.Persistence.Specifications.SortDirection;
using TriggerSpecs = Elsa.Persistence.Specifications.Triggers;
using WorkflowDefinitionIdSpecification = Elsa.Persistence.Specifications.WorkflowInstances.WorkflowDefinitionIdSpecification;
using DefinitionIdSpecification = Elsa.Persistence.Specifications.WorkflowDefinitions.WorkflowDefinitionIdSpecification;
using LogSpecs = Elsa.Persistence.Specifications.WorkflowExecutionLogRecords;
using WebhookSpecs = Elsa.Activities.Webhooks.Persistence.Specifications.WebhookDefinitions;

namespace Elsa.Core.IntegrationTests.Persistence.MongoDb
{
    /// <summary>
    /// Runs every query specification Elsa uses against the real MongoDB stores and compares the
    /// result with the same expression evaluated in memory (LINQ to Objects). This guards the
    /// translation of Elsa's LINQ expressions by the MongoDB driver (LINQ2 on driver 2.x, LINQ3 on 3.x).
    /// Requires a MongoDB instance (TEST_MONGODB, default mongodb://localhost:27017).
    /// </summary>
    public class MongoDbSpecificationTests : IAsyncLifetime
    {
        private static readonly Instant BaseTime = Instant.FromUtc(2024, 1, 1, 0, 0, 0);
        private static readonly string[] Tenants = { null!, "tenant-1", "tenant-2" };

        private readonly ITestOutputHelper _output;
        private readonly string _connectionString = Environment.GetEnvironmentVariable("TEST_MONGODB") ?? "mongodb://localhost:27017";
        private readonly string _databaseName = $"elsa_spec_tests_{Guid.NewGuid():N}";
        private readonly List<string> _failures = new();
        private ServiceProvider _services = default!;

        public MongoDbSpecificationTests(ITestOutputHelper output) => _output = output;

        public Task InitializeAsync()
        {
            var services = new ServiceCollection().AddLogging();

            void ConfigureMongo(Elsa.Persistence.MongoDb.Options.ElsaMongoDbOptions options)
            {
                options.ConnectionString = _connectionString;
                options.DatabaseName = _databaseName;
            }

            services.AddElsa(elsa => elsa.UseMongoDbPersistence(ConfigureMongo));
            new WebhookOptionsBuilder(services).UseWebhookMongoDbPersistence(ConfigureMongo);
            new SecretsOptionsBuilder(services).UseSecretsMongoDbPersistence(ConfigureMongo);
            new WorkflowSettingsOptionsBuilder(services).UseWorkflowSettingsMongoDbPersistence(ConfigureMongo);

            _services = services.BuildServiceProvider();
            return Task.CompletedTask;
        }

        public async Task DisposeAsync()
        {
            await new MongoClient(_connectionString).DropDatabaseAsync(_databaseName);
            await _services.DisposeAsync();
        }

        private TStore Store<TStore>() where TStore : class => _services.GetService<TStore>() ?? ActivatorUtilities.CreateInstance<TStore>(_services);

        [Fact(DisplayName = "Every Elsa query specification returns the same result from MongoDB as in memory")]
        public async Task SpecificationsMatchInMemoryEvaluation()
        {
            await CheckWorkflowDefinitions();
            await CheckWorkflowInstances();
            await CheckBookmarks();
            await CheckTriggers();
            await CheckExecutionLog();
            await CheckWebhooks();
            await CheckWorkflowSettings();
            await CheckSecrets();

            Assert.True(_failures.Count == 0, $"{_failures.Count} MongoDB query mismatch(es):\n" + string.Join("\n", _failures));
        }

        private async Task CheckWorkflowDefinitions()
        {
            var store = Store<MongoDbWorkflowDefinitionStore>();
            var data = new List<WorkflowDefinition>();

            for (var d = 0; d < 6; d++)
            for (var v = 1; v <= 3; v++)
                data.Add(new WorkflowDefinition
                {
                    Id = $"def-{d}-v{v}",
                    DefinitionId = $"def-{d}",
                    Version = v,
                    // def-0..def-2: v3 latest+published; def-3,def-4: v3 latest draft, v2 published; def-5: v3 latest, nothing published
                    IsLatest = v == 3,
                    IsPublished = d < 3 ? v == 3 : d < 5 && v == 2,
                    Name = $"Workflow{d}",
                    DisplayName = d % 2 == 0 ? $"Display a.b {d}" : $"Display (x) {d}",
                    Description = $"Description for {d} v{v}",
                    Tag = d % 3 == 0 ? "tag-a" : "tag-b",
                    TenantId = Tenants[d % 3],
                    CreatedAt = BaseTime.Plus(Duration.FromMinutes(d * 10 + v)),
                });

            await store.AddManyAsync(data);

            var versionOptions = new[] { VersionOptions.All, VersionOptions.Draft, VersionOptions.Latest, VersionOptions.Published, VersionOptions.LatestOrPublished, VersionOptions.SpecificVersion(2) };

            foreach (var vo in versionOptions)
            {
                await Check(store, data, $"VersionOptions({vo})", new VersionOptionsSpecification(vo));
                await Check(store, data, $"WorkflowDefinitionId(def-3, {vo})", new DefinitionIdSpecification("def-3", vo), allowEmpty: vo.IsDraft);
                await Check(store, data, $"WorkflowDefinitionName(Workflow4, {vo})", new WorkflowDefinitionNameSpecification("Workflow4", vo));
                await Check(store, data, $"WorkflowDefinitionTag(tag-a, {vo})", new WorkflowDefinitionTagSpecification("tag-a", vo));
                await Check(store, data, $"ManyWorkflowDefinitionIds({vo})", new ManyWorkflowDefinitionIdsSpecification(new[] { "def-1", "def-4", "def-5" }, vo));
            }

            await Check(store, data, "WorkflowDefinitionId with tenant", new DefinitionIdSpecification("def-1", VersionOptions.Latest, "tenant-1"));
            await Check(store, data, "WorkflowDefinitionName with tenant", new WorkflowDefinitionNameSpecification("Workflow2", null, "tenant-2"));
            await Check(store, data, "LatestOrPublishedWorkflowDefinitionId", new LatestOrPublishedWorkflowDefinitionIdSpecification("def-4"));
            await Check(store, data, "ManyWorkflowDefinitionNames", new ManyWorkflowDefinitionNamesSpecification(new[] { "Workflow0", "Workflow5" }));
            await Check(store, data, "ManyWorkflowDefinitionVersionIds", new ManyWorkflowDefinitionVersionIdsSpecification(new[] { "def-0-v1", "def-5-v3" }));
            await Check(store, data, "WorkflowDefinitionVersionId", new WorkflowDefinitionVersionIdSpecification("def-2-v2"));
            await Check(store, data, "SearchTerm plain", new WorkflowDefinitionSearchTermSpecification("Workflow3"));
            await Check(store, data, "SearchTerm with '.'", new WorkflowDefinitionSearchTermSpecification("a.b"));
            await Check(store, data, "SearchTerm with '('", new WorkflowDefinitionSearchTermSpecification("(x)"));
            await Check(store, data, "Tenant(null)", new TenantSpecification<WorkflowDefinition>(null));
            await Check(store, data, "Tenant(tenant-2)", new TenantSpecification<WorkflowDefinition>("tenant-2"));
            await Check(store, data, "EntityId", new EntityIdSpecification<WorkflowDefinition>("def-1-v1"));
            await Check(store, data, "Identity", Specification<WorkflowDefinition>.Identity);
            await Check(store, data, "None", Specification<WorkflowDefinition>.None, allowEmpty: true);
            await Check(store, data, "And", new VersionOptionsSpecification(VersionOptions.Published).And(new WorkflowDefinitionTagSpecification("tag-b")));
            await Check(store, data, "Or", new WorkflowDefinitionVersionIdSpecification("def-0-v1").Or(new WorkflowDefinitionVersionIdSpecification("def-5-v2")));
            await Check(store, data, "Not", new VersionOptionsSpecification(VersionOptions.Latest).Not());

            // Ordering + paging as used by the workflow definition list endpoint.
            await CheckOrdered(store, data, "Latest ordered by Name desc, page 1", new VersionOptionsSpecification(VersionOptions.Latest),
                new OrderBy<WorkflowDefinition>(x => x.Name!, SortDirection.Descending), new Paging(2, 3), q => q.OrderByDescending(x => x.Name));
            await CheckOrdered(store, data, "All ordered by CreatedAt asc, page 2", Specification<WorkflowDefinition>.Identity,
                new OrderBy<WorkflowDefinition>(x => x.CreatedAt, SortDirection.Ascending), Paging.Page(2, 5), q => q.OrderBy(x => x.CreatedAt));
            await CheckOrdered(store, data, "Published ordered by Version desc", new VersionOptionsSpecification(VersionOptions.Published),
                new OrderBy<WorkflowDefinition>(x => x.Id, SortDirection.Descending), null, q => q.OrderByDescending(x => x.Id));

            // Projection (FindManyAsync<TOut>).
            var spec = new VersionOptionsSpecification(VersionOptions.LatestOrPublished);
            var projected = (await store.FindManyAsync(spec, x => new DefinitionSummary { Id = x.Id, Name = x.Name, Version = x.Version })).Select(x => $"{x.Id}|{x.Name}|{x.Version}").OrderBy(x => x).ToList();
            var expectedProjection = data.AsQueryable().Where(spec.ToExpression()).Select(x => $"{x.Id}|{x.Name}|{x.Version}").OrderBy(x => x).ToList();
            Compare("WorkflowDefinition projection", expectedProjection, projected);
        }

        private async Task CheckWorkflowInstances()
        {
            var store = Store<MongoDbWorkflowInstanceStore>();
            var statuses = (WorkflowStatus[])Enum.GetValues(typeof(WorkflowStatus));
            var data = new List<WorkflowInstance>();

            for (var i = 0; i < 36; i++)
                data.Add(new WorkflowInstance
                {
                    Id = $"wi-{i:00}",
                    DefinitionId = $"def-{i % 4}",
                    DefinitionVersionId = $"def-{i % 4}-v{1 + i % 2}",
                    Version = 1 + i % 2,
                    TenantId = Tenants[i % 3],
                    WorkflowStatus = statuses[i % statuses.Length],
                    CorrelationId = $"corr-{i % 5}",
                    ContextType = i % 3 == 0 ? "Acme.Orders.Order, Acme" : "Acme.Invoices.Invoice, Acme",
                    ContextId = $"ctx-{i % 7}",
                    Name = i % 2 == 0 ? $"Order flow {i}" : $"Invoice a.b flow {i}",
                    CreatedAt = BaseTime.Plus(Duration.FromHours(i)),
                    LastExecutedAt = BaseTime.Plus(Duration.FromHours(i + 1)),
                });

            await store.AddManyAsync(data);

            foreach (var status in statuses)
                await Check(store, data, $"WorkflowStatus({status})", new WorkflowStatusSpecification(status));

            await Check(store, data, "WorkflowInstanceId", new WorkflowInstanceIdSpecification("wi-07"));
            await Check(store, data, "WorkflowInstanceIds", new WorkflowInstanceIdsSpecification(new[] { "wi-01", "wi-02", "wi-30" }));
            await Check(store, data, "ManyWorkflowInstanceIds", new ManyWorkflowInstanceIdsSpecification(new[] { "wi-05", "wi-35" }));
            await Check(store, data, "WorkflowDefinitionId", new WorkflowDefinitionIdSpecification("def-2"));
            await Check(store, data, "WorkflowDefinitionVersionIds", new WorkflowDefinitionVersionIdsSpecification(new[] { "def-1-v2", "def-3-v1" }));
            await Check(store, data, "CorrelationIds", new CorrelationIdsSpecification("def-1", new[] { "corr-0", "corr-3" }));
            await Check(store, data, "WorkflowInstanceCorrelationId", new WorkflowInstanceCorrelationIdSpecification("def-0", "corr-2"));
            await Check(store, data, "CorrelationId<T>", new CorrelationIdSpecification<WorkflowInstance>("corr-4"));
            await Check(store, data, "Unfinished", new UnfinishedWorkflowSpecification());
            await Check(store, data, "UnfinishedStatus", new WorkflowUnfinishedStatusSpecification());
            await Check(store, data, "FinishedStatus", new WorkflowFinishedStatusSpecification());
            await Check(store, data, "IsAlreadyExecuting", new WorkflowIsAlreadyExecutingSpecification());
            await Check(store, data, "CreatedBefore", new WorkflowCreatedBeforeSpecification(BaseTime.Plus(Duration.FromHours(20))));
            await Check(store, data, "CreatedBefore (exact boundary)", new WorkflowCreatedBeforeSpecification(BaseTime.Plus(Duration.FromHours(10))));
            await Check(store, data, "ContextMatch", new WorkflowInstanceContextMatchSpecification("Orders.Order"));
            await Check(store, data, "ContextIdMatch", new WorkflowInstanceContextIdMatchSpecification("Invoice", "ctx-3"));
            await Check(store, data, "NameMatch", new WorkflowInstanceNameMatchSpecification("Order flow"));
            await Check(store, data, "NameMatch with '.'", new WorkflowInstanceNameMatchSpecification("a.b"));
            await Check(store, data, "SearchTerm on name", new WorkflowSearchTermSpecification("flow 1"));
            await Check(store, data, "SearchTerm on id", new WorkflowSearchTermSpecification("wi-2"));
            await Check(store, data, "SearchTerm on context id", new WorkflowSearchTermSpecification("ctx-6"));
            await Check(store, data, "SearchTerm on correlation id", new WorkflowSearchTermSpecification("corr-1"));
            await Check(store, data, "Tenant(null)", new TenantSpecification<WorkflowInstance>(null));
            await Check(store, data, "Tenant(tenant-1)", new TenantSpecification<WorkflowInstance>("tenant-1"));
            await Check(store, data, "StatusFilter(Finished, Cancelled, Faulted)", new WorkflowStatusFilterSpecification(WorkflowStatus.Finished, WorkflowStatus.Cancelled, WorkflowStatus.Faulted));
            await Check(store, data, "StatusFilter(Suspended)", new WorkflowStatusFilterSpecification(WorkflowStatus.Suspended));

            // Workflow instance list endpoint: filter + ordering + paging.
            await CheckOrdered(store, data, "By definition ordered by CreatedAt desc", new WorkflowDefinitionIdSpecification("def-1"),
                new OrderBy<WorkflowInstance>(x => x.CreatedAt, SortDirection.Descending), new Paging(1, 4), q => q.OrderByDescending(x => x.CreatedAt));
            await CheckOrdered(store, data, "Unfinished ordered by LastExecutedAt asc", new UnfinishedWorkflowSpecification(),
                new OrderBy<WorkflowInstance>(x => x.LastExecutedAt!, SortDirection.Ascending), Paging.Page(0, 10), q => q.OrderBy(x => x.LastExecutedAt));
            await CheckOrdered(store, data, "All ordered by Name asc", Specification<WorkflowInstance>.Identity,
                new OrderBy<WorkflowInstance>(x => x.Name!, SortDirection.Ascending), Paging.Page(1, 7), q => q.OrderBy(x => x.Name));

            // Exactly what Elsa.Retention's CleanupJob does: find ids (projection, ordered, paged), then delete them.
            var threshold = BaseTime.Plus(Duration.FromHours(24));
            var retentionSpec = new WorkflowCreatedBeforeSpecification(threshold).And(new WorkflowStatusFilterSpecification(WorkflowStatus.Finished, WorkflowStatus.Cancelled, WorkflowStatus.Faulted));
            var retentionOrder = new OrderBy<WorkflowInstance>(x => x.CreatedAt, SortDirection.Descending);
            var expectedRetentionIds = data.AsQueryable().Where(retentionSpec.ToExpression()).OrderByDescending(x => x.CreatedAt).Take(3).Select(x => x.Id).ToList();
            var retentionIds = (await store.FindManyAsync(retentionSpec, wf => wf.Id, retentionOrder, new Paging(0, 3))).ToList();
            Compare("Retention: ids to delete (ordered)", expectedRetentionIds, retentionIds, ordered: true);

            var expectedDeleted = data.AsQueryable().Where(retentionSpec.ToExpression()).Count();
            var deleted = await store.DeleteManyAsync(retentionSpec);
            if (deleted != expectedDeleted)
                _failures.Add($"Retention: DeleteManyAsync deleted {deleted}, expected {expectedDeleted}");
            var remaining = data.Where(x => !retentionSpec.ToExpression().Compile()(x)).ToList();
            await Check(store, remaining, "After retention delete: all remaining", Specification<WorkflowInstance>.Identity);
        }

        private async Task CheckBookmarks()
        {
            var store = Store<MongoDbBookmarkStore>();
            var data = Enumerable.Range(0, 24).Select(i => new Bookmark
            {
                Id = $"bm-{i:00}",
                TenantId = Tenants[i % 3],
                Hash = $"hash-{i % 6}",
                Model = "{}",
                ModelType = i % 2 == 0 ? "Elsa.Activities.Signaling.Models.SignalReceivedBookmark, Elsa.Activities.Signaling" : "Elsa.Activities.Http.Bookmarks.HttpEndpointBookmark, Elsa.Activities.Http",
                ActivityType = i % 2 == 0 ? "SignalReceived" : "HttpEndpoint",
                ActivityId = $"act-{i}",
                WorkflowInstanceId = $"wi-{i % 8}",
                CorrelationId = $"corr-{i % 4}",
            }).ToList();

            await store.AddManyAsync(data);

            await Check(store, data, "BookmarkHash", new BookmarkSpecs.BookmarkHashSpecification("hash-2", "SignalReceived", "tenant-2"));
            await Check(store, data, "BookmarkHash null tenant", new BookmarkSpecs.BookmarkHashSpecification("hash-0", "SignalReceived", null));
            await Check(store, data, "BookmarkIds", new BookmarkSpecs.BookmarkIdsSpecification(new[] { "bm-01", "bm-10" }));
            await Check(store, data, "Bookmark without correlation", new BookmarkSpecs.BookmarkSpecification("HttpEndpoint", "tenant-1", null));
            await Check(store, data, "Bookmark with correlation", new BookmarkSpecs.BookmarkSpecification("SignalReceived", null, "corr-0"));
            await Check(store, data, "BookmarkTypeAndWorkflowInstance", new BookmarkSpecs.BookmarkTypeAndWorkflowInstanceSpecification(data[3].ModelType, "wi-3"));
            await Check(store, data, "BookmarkType", new BookmarkSpecs.BookmarkTypeSpecification(data[0].ModelType, null));
            await Check(store, data, "CorrelationId", new BookmarkSpecs.CorrelationIdSpecification("corr-1"));
            await Check(store, data, "WorkflowInstanceId", new BookmarkSpecs.WorkflowInstanceIdSpecification("wi-5"));
            await Check(store, data, "WorkflowInstanceIds", new BookmarkSpecs.WorkflowInstanceIdsSpecification(new[] { "wi-0", "wi-7" }));

            var deleteSpec = new BookmarkSpecs.WorkflowInstanceIdSpecification("wi-2");
            var expectedDeleted = data.Count(x => x.WorkflowInstanceId == "wi-2");
            var deleted = await store.DeleteManyAsync(deleteSpec);
            if (deleted != expectedDeleted)
                _failures.Add($"Bookmark DeleteManyAsync deleted {deleted}, expected {expectedDeleted}");
        }

        private async Task CheckTriggers()
        {
            var store = Store<MongoDbTriggerStore>();
            var data = Enumerable.Range(0, 18).Select(i => new Trigger
            {
                Id = $"tr-{i:00}",
                TenantId = Tenants[i % 3],
                Hash = $"hash-{i % 5}",
                Model = "{}",
                ModelType = i % 2 == 0 ? "Elsa.Activities.Http.Bookmarks.HttpEndpointBookmark, Elsa.Activities.Http" : "Elsa.Activities.Temporal.Bookmarks.CronBookmark, Elsa.Activities.Temporal.Common",
                ActivityType = i % 2 == 0 ? "HttpEndpoint" : "Cron",
                ActivityId = $"act-{i}",
                WorkflowDefinitionId = $"def-{i % 4}",
            }).ToList();

            await store.AddManyAsync(data);

            await Check(store, data, "TriggerIds", new TriggerSpecs.TriggerIdsSpecification(new[] { "tr-00", "tr-17" }));
            await Check(store, data, "TriggerModelType", new TriggerSpecs.TriggerModelTypeSpecification(data[1].ModelType));
            await Check(store, data, "TriggerModelType with tenant", new TriggerSpecs.TriggerModelTypeSpecification(data[0].ModelType, "tenant-2"));
            await Check(store, data, "Trigger", new TriggerSpecs.TriggerSpecification("HttpEndpoint", new[] { "hash-0", "hash-2", "hash-4" }, null));
            await Check(store, data, "Trigger with tenant", new TriggerSpecs.TriggerSpecification("Cron", new[] { "hash-1", "hash-3" }, "tenant-1"));
            await Check(store, data, "WorkflowDefinitionId", new TriggerSpecs.WorkflowDefinitionIdSpecification("def-3"));
            await Check(store, data, "WorkflowDefinitionIds", new TriggerSpecs.WorkflowDefinitionIdsSpecification(new[] { "def-0", "def-2" }));
        }

        private async Task CheckExecutionLog()
        {
            var store = Store<MongoDbWorkflowExecutionLogStore>();
            var data = Enumerable.Range(0, 30).Select(i => new WorkflowExecutionLogRecord(
                $"log-{i:00}", Tenants[i % 3], $"wi-{i % 5}", $"act-{i % 6}", i % 2 == 0 ? "WriteLine" : "HttpEndpoint",
                BaseTime.Plus(Duration.FromSeconds(i * 7)), i % 3 == 0 ? "Executed" : "Executing", $"message {i}")).ToList();

            await store.AddManyAsync(data);

            await Check(store, data, "ActivityId", new LogSpecs.ActivityIdSpecification("act-4"));
            await Check(store, data, "ActivityType", new LogSpecs.ActivityTypeSpecification("HttpEndpoint"));
            await Check(store, data, "WorkflowInstanceId", new LogSpecs.WorkflowInstanceIdSpecification("wi-3"));
            await CheckOrdered(store, data, "Log of instance ordered by Timestamp", new LogSpecs.WorkflowInstanceIdSpecification("wi-1"),
                new OrderBy<WorkflowExecutionLogRecord>(x => x.Timestamp, SortDirection.Ascending), new Paging(1, 3), q => q.OrderBy(x => x.Timestamp));
        }

        private async Task CheckWebhooks()
        {
            var store = Store<MongoDbWebhookDefinitionStore>();
            var data = Enumerable.Range(0, 6).Select(i => new WebhookDefinition
            {
                Id = $"wh-{i}",
                TenantId = Tenants[i % 3],
                Name = $"Webhook {i}",
                Path = $"/hooks/{i}",
                Description = $"Webhook number {i}",
                IsEnabled = i % 2 == 0,
            }).ToList();

            await store.AddManyAsync(data);

            await Check(store, data, "WebhookId", new WebhookSpecs.WebhookIdSpecification("wh-4"));
            await Check(store, data, "Webhook Tenant(tenant-1)", new TenantSpecification<WebhookDefinition>("tenant-1"));
            await Check(store, data, "Webhook Identity", Specification<WebhookDefinition>.Identity);
            await CheckOrdered(store, data, "Webhooks ordered by Name desc", Specification<WebhookDefinition>.Identity,
                new OrderBy<WebhookDefinition>(x => x.Name, SortDirection.Descending), new Paging(1, 3), q => q.OrderByDescending(x => x.Name));
        }

        private async Task CheckWorkflowSettings()
        {
            var store = Store<MongoDbWorkflowSettingsStore>();
            var data = Enumerable.Range(0, 9).Select(i => new WorkflowSetting
            {
                Id = $"ws-{i}",
                WorkflowBlueprintId = $"def-{i % 3}",
                Key = i % 2 == 0 ? "disabled" : "timeout",
                Value = i.ToString(),
            }).ToList();

            await store.AddManyAsync(data);

            await Check(store, data, "WorkflowSettingsId", new WorkflowSettingsIdSpecification("ws-5"));
            await Check(store, data, "WorkflowSettingsBlueprintId", new WorkflowSettingsBlueprintIdSpecification("def-1", "timeout"));
        }

        private async Task CheckSecrets()
        {
            var store = Store<MongoDbSecretsStore>();
            var data = Enumerable.Range(0, 8).Select(i => new Secret
            {
                Id = $"sec-{i}",
                Name = $"secret-{i}",
                DisplayName = $"Secret {i}",
                Type = i % 2 == 0 ? "MSSQLServer" : "Authorization",
                Properties = new List<SecretProperty>(),
            }).ToList();

            await store.AddManyAsync(data);

            await Check(store, data, "SecretsId", new SecretsIdSpecification("sec-3"));
            await Check(store, data, "SecretsName", new SecretsNameSpecification("secret-6"));
            await Check(store, data, "SecretType", new SecretTypeSpecification("Authorization"));
        }

        private async Task Check<T>(MongoDbStore<T> store, IReadOnlyCollection<T> data, string name, ISpecification<T> specification, bool allowEmpty = false) where T : class, IEntity
        {
            var label = $"{typeof(T).Name}: {name}";
            try
            {
                var expected = data.AsQueryable().Where(specification.ToExpression()).Select(x => x.Id).OrderBy(x => x).ToList();

                if (expected.Count == 0 && !allowEmpty)
                    _failures.Add($"{label}: test data does not match anything (test would be vacuous)");

                var actual = (await store.FindManyAsync(specification)).Select(x => x.Id).OrderBy(x => x).ToList();
                Compare(label, expected, actual);

                var count = await store.CountAsync(specification);
                if (count != expected.Count)
                    _failures.Add($"{label}: CountAsync returned {count}, expected {expected.Count}");

                var first = await store.FindAsync(specification);
                if (expected.Count == 0 ? first != null : first == null || !expected.Contains(first.Id))
                    _failures.Add($"{label}: FindAsync returned '{first?.Id}', expected one of [{string.Join(", ", expected)}]");

                _output.WriteLine($"OK   {label} ({expected.Count})");
            }
            catch (Exception e)
            {
                _failures.Add($"{label}: threw {e.GetType().Name}: {e.Message}");
            }
        }

        private async Task CheckOrdered<T>(MongoDbStore<T> store, IReadOnlyCollection<T> data, string name, ISpecification<T> specification, IOrderBy<T> orderBy, IPaging? paging,
            Func<IQueryable<T>, IOrderedQueryable<T>> inMemoryOrder) where T : class, IEntity
        {
            var label = $"{typeof(T).Name}: {name}";
            try
            {
                var query = inMemoryOrder(data.AsQueryable().Where(specification.ToExpression())).AsQueryable();
                if (paging != null)
                    query = query.Skip(paging.Skip).Take(paging.Take);
                var expected = query.Select(x => x.Id).ToList();

                if (expected.Count == 0)
                    _failures.Add($"{label}: test data does not match anything (test would be vacuous)");

                var actual = (await store.FindManyAsync(specification, orderBy, paging)).Select(x => x.Id).ToList();
                Compare(label, expected, actual, ordered: true);

                var projected = (await store.FindManyAsync(specification, x => x.Id, orderBy, paging)).ToList();
                Compare(label + " (projected ids)", expected, projected, ordered: true);

                _output.WriteLine($"OK   {label} ({expected.Count}, ordered)");
            }
            catch (Exception e)
            {
                _failures.Add($"{label}: threw {e.GetType().Name}: {e.Message}");
            }
        }

        private void Compare(string label, IList<string> expected, IList<string> actual, bool ordered = false)
        {
            if (!expected.SequenceEqual(actual))
                _failures.Add($"{label}: expected{(ordered ? " (ordered)" : "")} [{string.Join(", ", expected)}] but MongoDB returned [{string.Join(", ", actual)}]");
        }

        public class DefinitionSummary
        {
            public string Id { get; set; } = default!;
            public string? Name { get; set; }
            public int Version { get; set; }
        }
    }
}
