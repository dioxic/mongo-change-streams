package com.mongodb.csp;

import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.csp.processors.Processor;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Predicate;

import static com.mongodb.client.model.Filters.eq;

public class SimpleWorker extends AbstractWorker {

    public SimpleWorker(Boolean logDupKeyEx,
                        Processor processor,
                        MongoCollection<Document> sourceCollection,
                        MongoCollection<Document> targetCollection,
                        MongoCollection<Document> tokenCollection,
                        MongoCollection<Document> errorCollection) {
        super(logDupKeyEx, processor, sourceCollection, targetCollection, tokenCollection, errorCollection);
    }

    private final Predicate<Throwable> exPredicate = e -> (e instanceof MongoServerException)
            && (!(e instanceof DuplicateKeyException) || logDupKeyEx);

    public void run() {

        var pipeline = processor.getChangeStreamPipeline();
        var resumeToken = getResumeToken();
        var publisher = sourceCollection.watch(pipeline);

        if (resumeToken != null) {
            publisher = publisher.resumeAfter(resumeToken);
        }

        Flux.from(publisher)
                .doOnSubscribe(subscriber -> logger.info("Subscribed to change stream on '{}' with pipeline {}", sourceCollection.getNamespace(), pipeline))
                .doOnComplete(() -> logger.info("Completed change stream"))
                .doOnCancel(() -> logger.info("Cancelled change stream"))
                .doOnNext(event -> logger.debug("Processing event: {}", event))
                .concatMap(event -> processor.handle(targetCollection, event)
                        .doOnError(exPredicate, e -> {
                            var errDoc = createErrorDocument((MongoServerException) e, event);
                            Mono.from(errorCollection.insertOne(errDoc)).block();
                        })
                        .onErrorResume(MongoWriteException.class, e -> Mono.empty())
                        .flatMap(res -> {
                            var tokenDoc = createTokenDocument(event);
                            return Mono.from(tokenCollection.replaceOne(
                                    eq("_id", tokenDoc.get("_id")),
                                    tokenDoc,
                                    new ReplaceOptions().upsert(true))
                            );
                        })
                )
                .blockLast();
    }

}
