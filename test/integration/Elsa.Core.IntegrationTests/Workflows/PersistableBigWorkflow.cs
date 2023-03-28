using Elsa.Activities.UserTask.Activities;
using Elsa.Builders;
using Elsa.Activities.ControlFlow;
using Elsa.Activities.Primitives;
using Elsa.Models;

namespace Elsa.Core.IntegrationTests.Workflows
{
    public class PersistableBigWorkflow : IWorkflow
    {
        public static readonly object Result = new object();

        public virtual void Build(IWorkflowBuilder builder)
        {
            //create a 24 MB variable, really big variable that can trigger problem/limitation in persistence layer (ex mongodb limitation of 16MB document size)
            builder
                .StartWith<SetVariable>(a => a.Set(x => x.VariableName, "BigBig").Set(x => x.Value, new string('a', 1024 * 1024 * 24)))
                .Then<UserTask>(setup: s => {
                    s.Set(x => x.Actions, c => new [] { new string('B', 1024 * 1024 * 24) });
                })
                .Finish(x => x.WithOutput(Result));
        }

        public class OnSuspend : PersistableBigWorkflow
        {
            public override void Build(IWorkflowBuilder builder)
            {
                builder.WithPersistenceBehavior(WorkflowPersistenceBehavior.Suspended);
                base.Build(builder);
            }
        }

        public class OnActivityExecuted : PersistableBigWorkflow
        {
            public override void Build(IWorkflowBuilder builder)
            {
                builder.WithPersistenceBehavior(WorkflowPersistenceBehavior.ActivityExecuted);
                base.Build(builder);
            }
        }

        public class OnWorkflowBurst : PersistableBigWorkflow
        {
            public override void Build(IWorkflowBuilder builder)
            {
                builder.WithPersistenceBehavior(WorkflowPersistenceBehavior.WorkflowBurst);
                base.Build(builder);
            }
        }
    }
}