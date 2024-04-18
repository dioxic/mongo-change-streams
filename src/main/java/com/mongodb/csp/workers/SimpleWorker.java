package com.mongodb.csp.workers;

import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoServerException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.csp.processors.Processor;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

public class SimpleWorker extends AbstractWorker {

    public SimpleWorker(Integer workerId,
                        Integer totalWorkers,
                        Boolean logDupKeyEx,
                        Processor processor,
                        MongoCollection<Document> sourceCollection,
                        MongoCollection<Document> targetCollection,
                        MongoCollection<Document> tokenCollection,
                        MongoCollection<Document> errorCollection) {
        super(workerId, totalWorkers, logDupKeyEx, processor, sourceCollection, targetCollection, tokenCollection, errorCollection);
    }

    public Integer call() {
        var upsertReplaceOptions = new ReplaceOptions().upsert(true);
        var pipeline = getChangeStreamPipeline();
        var resumeToken = getResumeToken();

        logger.info("Initiating change stream with pipeline: {}", pipeline);
        var cs = sourceCollection.watch(pipeline);

        if (resumeToken != null) {
            logger.info("Change stream resuming from {}", resumeToken);
            cs = cs.resumeAfter(resumeToken);
        }

        cs.forEach(event -> {
            try {
                logger.debug("Handing changestream msg {}", event);
                processor.handle(targetCollection, event);
                var tokenDoc = createTokenDocument(event);
                logger.debug("Persisting resume token {}", tokenDoc);
                tokenCollection.replaceOne(
                        eq("_id", tokenDoc.get("_id")),
                        tokenDoc,
                        upsertReplaceOptions
                );
            } catch (MongoServerException e) {
                if (!(e instanceof DuplicateKeyException) || logDupKeyEx) {
                    var errDoc = createErrorDocument(e, event);
                    errorCollection.insertOne(errDoc);
                    throw new RuntimeException(e);
                } else {
                    logger.trace("Ignoring duplicate key exception");
                }
            }
        });

        logger.info("Change stream worker finished");

        return 0;
    }
}
