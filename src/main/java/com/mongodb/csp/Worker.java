package com.mongodb.csp;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.csp.processors.Processor;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

public class Worker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);
    private static final ReplaceOptions tokenUpdateOptions = new ReplaceOptions().upsert(true);
    private final Boolean logDupKeyEx;
    private final Processor processor;
    private final MongoCollection<Document> sourceCollection;
    private final MongoCollection<Document> targetCollection;
    private final MongoCollection<Document> tokenCollection;
    private final MongoCollection<Document> errorCollection;
    private final @Nullable BsonDocument resumeToken;

    public Worker(Boolean logDupKeyEx,
                  Processor processor,
                  MongoCollection<Document> sourceCollection,
                  MongoCollection<Document> targetCollection,
                  MongoCollection<Document> tokenCollection,
                  MongoCollection<Document> errorCollection,
                  @Nullable BsonDocument resumeToken) {
        this.logDupKeyEx = logDupKeyEx;
        this.processor = processor;
        this.sourceCollection = sourceCollection;
        this.targetCollection = targetCollection;
        this.tokenCollection = tokenCollection;
        this.errorCollection = errorCollection;
        this.resumeToken = resumeToken;
    }

    public void run() {
        var pipeline = processor.getChangeStreamPipeline();
        var publisher = sourceCollection.watch(pipeline);

        if (resumeToken != null) {
            publisher = publisher.resumeAfter(resumeToken);
        }

        Flux.from(publisher)
                .doOnSubscribe(subscriber -> logger.info("Subscribed to change stream on '{}' with pipeline {}", sourceCollection.getNamespace(), pipeline))
                .doOnComplete(() -> logger.info("Completed change stream"))
                .doOnCancel(() -> logger.info("Cancelled change stream"))
                .doOnNext(event -> logger.debug("Processing event: {}", event))
                .map(event -> pairTransform(event, processor))
                .bufferTimeout(100, Duration.ofSeconds(2))
                .doOnNext(batch -> logger.debug("Writing batch of {} events", batch.size()))
//                .concatMap(process)
                .concatMap(writeModelPairs -> {
                    var writeModels = writeModelPairs.stream().map(EventModelPair::second).toList();
                    return processor.write(targetCollection, writeModels)
                            .doOnError(MongoBulkWriteException.class, e -> {
                                var events = writeModelPairs.stream().map(EventModelPair::first).toList();
                                Flux.fromIterable(createErrorDocuments(e, events))
                                        .doOnNext(errDoc -> logger.error("{}", errDoc))
                                        .flatMap(errorCollection::insertOne)
                                        .subscribe();
                            })
                            .onErrorResume(MongoBulkWriteException.class, e -> Mono.just(e.getWriteResult()));
                })
                .doOnEach(System.out::println)
                .blockLast();
    }

    private Mono<BulkWriteResult> process(List<ChangeStreamDocument<Document>> events,
                                          List<WriteModel<Document>> writeModels) {
        return processor.write(targetCollection, writeModels)
                .doOnError(MongoBulkWriteException.class, e -> {
                    // log the error in the error collection
                    Flux.fromIterable(createErrorDocuments(e, events))
                            .doOnNext(errDoc -> logger.error("{}", errDoc))
                            .flatMap(errorCollection::insertOne)
                            .subscribe();

                    var unprocessed = getUnprocessedModels(e, events, writeModels);


                })
                .onErrorResume(MongoBulkWriteException.class, e -> Mono.just(e.getWriteResult()));
    }

    /**
     * Create error documents. Exclude duplicate key exception error category.
     *
     * @param bulkWriteException exception
     * @param events             change stream event list
     */
    private List<Document> createErrorDocuments(MongoBulkWriteException bulkWriteException,
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

    private List<EventModelPair<Document>> getUnprocessedModels(MongoBulkWriteException bulkWriteException,
                                                            List<ChangeStreamDocument<Document>> events,
                                                            List<WriteModel<Document>> writeModels) {
        if (events.size() != writeModels.size()) {
            throw new IllegalStateException("Only one error expected using Ordered=true");
        }

        var errIndexes = bulkWriteException.getWriteErrors().stream()
                .map(BulkWriteError::getIndex)
                .toList();

        if (errIndexes.size() > 1) {
            throw new IllegalStateException("Only one error expected using Ordered=true");
        }

        // get the write models and events that weren't processed because of the error
        return IntStream
                .range(Math.min(errIndexes.get(0) + 1, writeModels.size() - 1), writeModels.size())
                .mapToObj(i -> new EventModelPair<>(events.get(i), writeModels.get(i)))
                .toList();
    }

    private EventModelPair<Document> pairTransform(
            ChangeStreamDocument<Document> changeStreamDocument,
            Processor processor) {
        return new EventModelPair<>(changeStreamDocument, processor.transform(changeStreamDocument));
    }

}
