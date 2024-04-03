package com.mongodb.csp;

import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.csp.processors.Processor;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public abstract class AbstractWorker implements Runnable {
    protected static final Logger logger = LoggerFactory.getLogger(SimpleWorker.class);
    private static final ReplaceOptions tokenUpdateOptions = new ReplaceOptions().upsert(true);
    protected final Boolean logDupKeyEx;
    protected final Processor processor;
    protected final MongoCollection<Document> sourceCollection;
    protected final MongoCollection<Document> targetCollection;
    protected final MongoCollection<Document> tokenCollection;
    protected final MongoCollection<Document> errorCollection;
    protected final @Nullable BsonDocument resumeToken;

    public AbstractWorker(Boolean logDupKeyEx, Processor processor, MongoCollection<Document> sourceCollection, MongoCollection<Document> targetCollection, MongoCollection<Document> tokenCollection, MongoCollection<Document> errorCollection, @Nullable BsonDocument resumeToken) {
        this.logDupKeyEx = logDupKeyEx;
        this.processor = processor;
        this.sourceCollection = sourceCollection;
        this.targetCollection = targetCollection;
        this.tokenCollection = tokenCollection;
        this.errorCollection = errorCollection;
        this.resumeToken = resumeToken;
    }
}
