using Elsa.WorkflowSettings.Persistence.EntityFramework.Core;
using Microsoft.EntityFrameworkCore;

namespace Elsa.WorkflowSettings.Persistence.EntityFramework.MySql
{
    public static class WorkflowSettingsDbContextOptionsBuilderExtensions
    {
        /// <summary>
        /// Configures the context to use MySql
        /// </summary>
        public static DbContextOptionsBuilder UseWorkflowSettingsMySql(this DbContextOptionsBuilder builder, string connectionString) =>
            builder.UseMySQL(connectionString, db => db
                .MigrationsAssembly(typeof(WorkflowSettingsMySqlElsaContextFactory).Assembly.GetName().Name)
                .MigrationsHistoryTable(WorkflowSettingsContext.MigrationsHistoryTable, WorkflowSettingsContext.ElsaSchema));
    }
}
