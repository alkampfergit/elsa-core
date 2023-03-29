using Elsa.Models;
using Elsa.Persistence.Specifications;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using MongoDB.Driver.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Elsa.Persistence.MongoDb.Stores
{
    public class MongoDbWorkflowInstanceStore : MongoDbStore<WorkflowInstance>, IWorkflowInstanceStore
    {
        /// <summary>
        /// Keeping an instance of _blobStorage to save variables for workflow instances, this will
        /// reduce the size of mongodb and also will avoid risk of hitting 16 MB limit in case of big
        /// variable saved on a workflow instance.
        /// </summary>
        private readonly IMongoCollection<BsonDocument> _rawCollection;
        private readonly GridFSBucket<string> _gridFSBucket;

        public MongoDbWorkflowInstanceStore(
            IMongoCollection<WorkflowInstance> collection,
            Elsa.Services.IIdGenerator idGenerator) : base(collection, idGenerator)
        {
            _rawCollection = Collection.Database.GetCollection<BsonDocument>(Collection.CollectionNamespace.CollectionName);
            _gridFSBucket = new GridFSBucket<String>(Collection.Database, new GridFSBucketOptions()
            {
                BucketName = $"{Collection.CollectionNamespace.CollectionName}_GridFs",
            });
        }

        public override async Task SaveAsync(WorkflowInstance entity, CancellationToken cancellationToken = default)
        {
            BsonDocument doc = await GetBsonDocument(entity);

            await _rawCollection.ReplaceOneAsync(Builders<BsonDocument>.Filter.Eq("_id", entity.Id), doc, new ReplaceOptions { IsUpsert = true }, cancellationToken);
        }

        private async Task<BsonDocument> GetBsonDocument(WorkflowInstance entity)
        {
            if (entity.Id == null!)
                entity.Id = IdGenerator.Generate();

            //manually convert to bsonDocument
            var doc = entity.ToBsonDocument();
            //now variables must be removed if too big
            if (TryGetVariablesValue(doc, out var variablesContent))
            {
                if (variablesContent.Length > 1024 * 10)
                {
                    //move to gridf to avoid bloating the main collection or to exceed 16 mb document size
                    var key = GetGridFsBlobKey(entity.Id);
                    using var stream = _gridFSBucket.OpenUploadStream(key, "variables.json");
                    await stream.WriteAsync(Encoding.UTF8.GetBytes(variablesContent));
                    doc["Variables"] = BsonValue.Create(key);
                }
            }

            return doc;
        }

        private static string GetGridFsBlobKey(string workflowId)
        {
            return $"GRIDFS/{workflowId}";
        }

        public override async Task UpdateAsync(WorkflowInstance entity, CancellationToken cancellationToken = default)
        {
            BsonDocument doc = await GetBsonDocument(entity);
            await _rawCollection.ReplaceOneAsync(Builders<BsonDocument>.Filter.Eq("_id", entity.Id), doc, new ReplaceOptions { IsUpsert = false });
        }

        public override async Task AddManyAsync(IEnumerable<WorkflowInstance> entities, CancellationToken cancellationToken = default)
        {
            if (!entities.Any())
                return;

            List<BsonDocument> docs = new List<BsonDocument>();
            foreach (var entity in entities)
            {
                docs.Add(await GetBsonDocument(entity));
            }

            await _rawCollection.InsertManyAsync(docs, default, cancellationToken);
        }

        public override async Task DeleteAsync(WorkflowInstance entity, CancellationToken cancellationToken = default)
        {
            var doc = await _rawCollection.FindOneAndDeleteAsync(
                Builders<BsonDocument>.Filter.Eq("_id", entity.Id),
                new FindOneAndDeleteOptions<BsonDocument, BsonDocument>(),
                cancellationToken);

            if (TryGetVariablesValue(doc, out string variables) && variables.StartsWith("GRIDFS/"))
            {
                await _gridFSBucket.DeleteAsync(doc["Variables"].AsString);
            }
        }

        /// <summary>
        /// Try to safely grab Variables property from a bson document
        /// </summary>
        /// <param name="doc"></param>
        /// <param name="variables"></param>
        /// <returns></returns>
        private static bool TryGetVariablesValue(BsonDocument doc, out string variables)
        {
            variables = string.Empty;
            if (doc.Names.Contains("Variables") && doc["Variables"].BsonType == BsonType.String)
            {
                variables = doc["Variables"].AsString;
                return true;
            }

            return false;
        }

        public override async Task<int> DeleteManyAsync(ISpecification<WorkflowInstance> specification, CancellationToken cancellationToken = default)
        {
            //find all the object
            var filter = Builders<WorkflowInstance>.Filter.Where(specification.ToExpression());
            var idList = await Collection.Find(filter).Project(s => s.Id).ToListAsync();
            //now I need to delete everything then delete all the buckets.
            var result = await Collection.DeleteManyAsync(filter, cancellationToken);

            foreach (var id in idList)
            {
                await _gridFSBucket.DeleteAsync(GetGridFsBlobKey(id));
            }

            return (int)result.DeletedCount;
        }

        public override async Task<IEnumerable<WorkflowInstance>> FindManyAsync(ISpecification<WorkflowInstance> specification, IOrderBy<WorkflowInstance>? orderBy = default, IPaging? paging = default, CancellationToken cancellationToken = default)
        {
            //I need first to grab all the id
            var idList = await Collection.AsQueryable().Apply(specification).Apply(orderBy).Apply(paging).Select(e => e.Id).ToListAsync(cancellationToken);

            //now load all bson document
            var docsCursor = await _rawCollection.FindAsync(Builders<BsonDocument>.Filter.In("_id", idList));

            var docs = await docsCursor.ToListAsync();

            //now restore the original content for all docs 
            foreach (var doc in docs)
            {
                await RehydrateDoc(doc);
            }

            return docs.Select(d => BsonSerializer.Deserialize<WorkflowInstance>(d)).ToArray();
        }

        private async Task RehydrateDoc(BsonDocument? doc)
        {
            if (doc == null) return;

            if (TryGetVariablesValue(doc, out string variables) && variables.StartsWith("GRIDFS/"))
            {
                //reload data from bucket
                using var stream = _gridFSBucket.OpenDownloadStream(variables);
                if (stream != null)
                {
                    var raw = await stream.ReadBytesToEndAsync();
                    var originalValue = Encoding.UTF8.GetString(raw);
                    doc["Variables"] = BsonValue.Create(originalValue);
                }
                else
                {
                    doc["Variables"] = BsonValue.Create("{\"Data\":{}}");
                }
            }
        }

        public override async Task<WorkflowInstance?> FindAsync(ISpecification<WorkflowInstance> specification, CancellationToken cancellationToken = default) 
        {
            var id = await Collection.AsQueryable().Where(specification.ToExpression()).Select(e => e.Id).FirstOrDefaultAsync(cancellationToken);
            if (string.IsNullOrEmpty(id)) return null;

            var docCursor = await _rawCollection.FindAsync(Builders<BsonDocument>.Filter.Eq("_id", id));
            var doc = await docCursor.FirstOrDefaultAsync();

            await RehydrateDoc(doc);
            return BsonSerializer.Deserialize<WorkflowInstance>(doc);
        }
    }
}