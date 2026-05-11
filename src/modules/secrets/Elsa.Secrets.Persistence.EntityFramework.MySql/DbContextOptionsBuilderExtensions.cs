using Elsa.Secrets.Persistence.EntityFramework.Core;
using Microsoft.EntityFrameworkCore;

namespace Elsa.Secrets.Persistence.EntityFramework.MySql
{
    public static class DbContextOptionsBuilderExtensions
    {
        /// <summary>
        /// Configures the context to use MySql
        /// </summary>
        public static DbContextOptionsBuilder UseSecretsMySql(this DbContextOptionsBuilder builder, string connectionString) =>
            builder.UseMySQL(connectionString, db => db
                .MigrationsAssembly(typeof(MySqlContextFactory).Assembly.GetName().Name)
                .MigrationsHistoryTable(SecretsContext.MigrationsHistoryTable));
    }
}
