package com.mongodb.csp.processors;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;
import reactor.core.publisher.Mono;

import java.util.List;

public interface Processor {

    List<Bson> getChangeStreamPipeline();

    Document transform(ChangeStreamDocument<Document> changeStreamDocument);

    default Mono<?> handle(MongoCollection<Document> targetCollection,
                           ChangeStreamDocument<Document> changeStreamDocument) {

        return switch (changeStreamDocument.getOperationType()) {
            case INSERT -> write(targetCollection, transform(changeStreamDocument));
            case DELETE -> delete(targetCollection, changeStreamDocument.getDocumentKey().get("_id"));

            default -> throw new IllegalArgumentException(changeStreamDocument.getOperationType() + " not supported");
        };

    }

    default Mono<BulkWriteResult> write(MongoCollection<Document> targetCollection,
                                        List<WriteModel<Document>> writeModels) {
        return Mono.from(targetCollection.bulkWrite(writeModels));
    }

    default Mono<UpdateResult> write(MongoCollection<Document> targetCollection,
                                     Document doc) {
        return Mono.from(targetCollection.replaceOne(Filters.eq("_id", doc.get("_id")), doc, new ReplaceOptions().upsert(true)));
    }

    default Mono<DeleteResult> delete(MongoCollection<Document> targetCollection,
                                      Object docKey) {
        return Mono.from(targetCollection.deleteOne(Filters.eq("_id", docKey)));
    }

}
