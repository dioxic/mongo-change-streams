package com.mongodb.csp;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.csp.model.EventTransformPair;
import com.mongodb.csp.processors.Processor;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.Filters.eq;

public abstract class AbstractWorker implements Callable<Integer> {
    private static final ReplaceOptions tokenUpdateOptions = new ReplaceOptions().upsert(true);
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final Integer totalWorkers;
    protected final Integer workerId;
    protected final Boolean logDupKeyEx;
    protected final Processor processor;
    protected final MongoCollection<Document> sourceCollection;
    protected final MongoCollection<Document> targetCollection;
    protected final MongoCollection<Document> tokenCollection;
    protected final MongoCollection<Document> errorCollection;

    public AbstractWorker(Integer workerId,
                          Integer totalWorkers,
                          Boolean logDupKeyEx,
                          Processor processor,
                          MongoCollection<Document> sourceCollection,
                          MongoCollection<Document> targetCollection,
                          MongoCollection<Document> tokenCollection,
                          MongoCollection<Document> errorCollection) {
        this.logDupKeyEx = logDupKeyEx;
        this.totalWorkers = totalWorkers;
        this.workerId = workerId;
        this.processor = processor;
        this.sourceCollection = sourceCollection;
        this.targetCollection = targetCollection;
        this.tokenCollection = tokenCollection;
        this.errorCollection = errorCollection;
    }

    protected EventTransformPair<Document> pairTransform(
            ChangeStreamDocument<Document> changeStreamDocument,
            Processor processor) {
        return new EventTransformPair<>(changeStreamDocument, processor.transform(changeStreamDocument));
    }

    /**
     * Create error documents. Exclude duplicate key exception error category.
     *
     * @param bulkWriteException exception
     * @param events             change stream event list
     */
    protected List<Document> createErrorDocuments(MongoBulkWriteException bulkWriteException,
                                                  List<ChangeStreamDocument<Document>> events) {

        return bulkWriteException.getWriteErrors().stream()
                .filter(wex -> logDupKeyEx || wex.getCategory() != ErrorCategory.DUPLICATE_KEY)
                .map(wex -> {
                    var doc = new Document();
                    doc.put("errCode", wex.getCode());
                    doc.put("errCategory", wex.getCategory());
                    doc.put("errMessage", wex.getMessage());
                    doc.put("event", events.get(wex.getIndex()));
                    return doc;
                })
                .toList();
    }

    protected Document createErrorDocument(MongoException exception, ChangeStreamDocument<Document> event) {
        var doc = new Document();
        doc.put("errCode", exception.getCode());
        doc.put("errMessage", exception.getMessage());
        doc.put("event", event);
        return doc;
    }

    private String getTokenCollectionId() {
        return targetCollection.getNamespace().getFullName() + "_" + workerId;
    }

    protected Document createTokenDocument(ChangeStreamDocument<Document> event) {
        var doc = new Document();
        doc.put("_id", getTokenCollectionId());
        doc.put("workerId", workerId);
        doc.put("token", event.getResumeToken());
        doc.put("lastUpdateDate", new Date());
        return doc;
    }

    protected BsonDocument getResumeToken() {
        return tokenCollection.find(eq("_id", getTokenCollectionId()))
                .map(doc -> doc.get("token", Document.class).toBsonDocument())
                .first();
    }

    /**
     * Gets the initial pipeline from the processor and appends workerId filtering
     */
    protected List<Bson> getChangeStreamPipeline() {
        var pipeline = new ArrayList<>(processor.getChangeStreamPipeline());

        if (totalWorkers > 1) {
            logger.debug("Applying fan-out to worker {} pipeline", workerId);

            var workerIdCalc = addFields(new Field<>("workerId",
                    new Document("$mod", List.of(new Document("$abs", new Document("$toHashedIndexKey", "$_id")), totalWorkers)))
            );
            var workerIdFilter = Aggregates.match(new Document("workerId", workerId));

            pipeline.add(workerIdCalc);
            pipeline.add(workerIdFilter);
        }

        return pipeline;
    }
}
