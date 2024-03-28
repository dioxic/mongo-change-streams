package com.mongodb.csp.processors;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;
import reactor.core.publisher.Mono;

import java.util.List;

public interface Processor {

    List<Bson> getChangeStreamPipeline();

    WriteModel<Document> transform(ChangeStreamDocument<Document> changeStreamDocument);

    default Mono<BulkWriteResult> write(MongoCollection<Document> targetCollection,
                                             List<WriteModel<Document>> writeModels) {
        return Mono.from(targetCollection.bulkWrite(writeModels));
    }

}
