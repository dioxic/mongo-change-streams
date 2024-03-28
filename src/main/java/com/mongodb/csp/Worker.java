package com.mongodb.csp;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.csp.processors.Processor;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.function.BiConsumer;

public class Worker {

    private static final Logger logger = LoggerFactory.getLogger(Worker.class);
    private static final ReplaceOptions tokenUpdateOptions = new ReplaceOptions().upsert(true);

    public void doStuff(Processor processor,
                        MongoCollection<Document> sourceCollection,
                        MongoCollection<Document> targetCollection,
                        MongoCollection<Document> tokenCollection,
                        MongoCollection<Document> errorCollection,
                        @Nullable BsonDocument resumeToken) throws InterruptedException {

        var pipeline = processor.getChangeStreamPipeline();
        var publisher = sourceCollection.watch(pipeline);

        if (resumeToken != null) {
            publisher = publisher.resumeAfter(resumeToken);
        }

//        var latch = new CountDownLatch(1);
//        var errorSink = Sinks.many().unicast().onBackpressureBuffer(new LinkedBlockingQueue<MongoBulkWriteException>(100));
//        var errorQueue = new LinkedBlockingQueue<BulkWriteResult>(100);


        var processorResult = Flux.from(publisher)
                .doOnSubscribe(subscriber -> logger.info("Subscribed to change stream on '{}' with pipeline {}", sourceCollection.getNamespace(), pipeline))
                .doOnComplete(() -> logger.info("Completed change stream"))
                .doOnCancel(() -> logger.info("Cancelled change stream"))
                .doOnNext(event -> logger.debug("Processing event: {}", event))
                .map(event -> pairTransform(event, processor))
//                .map(processor::transform)
                .bufferTimeout(100, Duration.ofSeconds(2))
                .doOnNext(batch -> logger.debug("Writing batch of {} events", batch.size()))
                .concatMap(writeModelPairs -> {
                    var events = writeModelPairs.stream().map(EventModelPair::first).toList();
                    var writeModels = writeModelPairs.stream().map(EventModelPair::second).toList();
                    return Mono.from(processor.write(targetCollection, writeModels))
                            .doOnError(MongoBulkWriteException.class, e -> {
                                Flux.fromIterable(createErrorDocuments(e, events))
                                        .map(errorCollection::insertOne)
                                        .blockLast();
                            })
                            .onErrorResume(EventBatchException.class, e -> Mono.just(e.getMongoException().getWriteResult()));
//                            .onErrorMap(MongoBulkWriteException.class, e -> new EventBatchException(events, e));
                })
                .publish()
                .autoConnect(1);

        processorResult.subscribe(System.out::println);
        processorResult.blockLast();

//        var errorFlux = processorResult
//                .filter(eventBatchResult -> eventBatchResult.result() instanceof MongoBulkWriteException)
//                .map(ebr -> new EventBatchResult<>(ebr.events(), (MongoBulkWriteException) ebr.result()))
//                .subscribe(ebr -> System.out.println("something failed!!"));
//
//        var successFlux = processorResult
//                .filter(eventBatchResult -> eventBatchResult.result() instanceof BulkWriteResult)
//                .publish()
//                .autoConnect(2);

        // save token
//        successFlux.flatMap(ebr -> {
//                    var doc = new Document();
//                    var ns = targetCollection.getNamespace().getFullName();
//                    doc.append("token", ebr.getLatestResumeToken());
//                    return Mono.from(tokenCollection.replaceOne(Filters.eq("_id", ns), doc, tokenUpdateOptions));
//                })
//                .subscribe(System.out::println);

        // print metrics token
//        successFlux.subscribe(System.out::println);


    }

    private List<Document> createErrorDocuments(MongoBulkWriteException bulkWriteException, List<ChangeStreamDocument<Document>> events) {
        return bulkWriteException.getWriteErrors().stream()
                .map(wex -> {
                    var doc = new Document();
                    doc.put("errMessage", wex.getMessage());
                    doc.put("event", events.get(wex.getIndex()));
                    return doc;
                })
                .toList();
    }

    private EventModelPair<Document> pairTransform(
            ChangeStreamDocument<Document> changeStreamDocument,
            Processor processor) {
        return new EventModelPair<>(changeStreamDocument, processor.transform(changeStreamDocument));
    }

}
