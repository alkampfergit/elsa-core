using Elsa.Persistence.EntityFramework.Core;
using Microsoft.EntityFrameworkCore;

namespace Elsa.Persistence.EntityFramework.MySql
{
    public static class DbContextOptionsBuilderExtensions
    {
        /// <summary>
        /// Configures the context to use MySql
        /// </summary>
        public static DbContextOptionsBuilder UseMySql(this DbContextOptionsBuilder builder, string connectionString, ElsaDbContextOptions? options = default) =>
            builder.UseMySQL(connectionString, db => db
                .MigrationsAssembly(options.GetMigrationsAssemblyName(typeof(MySqlElsaContextFactory).Assembly))
                .MigrationsHistoryTable(options.GetMigrationsHistoryTableName(), options.GetSchemaName()));
    }
}
