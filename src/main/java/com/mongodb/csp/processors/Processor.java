package com.mongodb.csp.processors;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

public interface Processor {

    List<Bson> getChangeStreamPipeline();

    Document transform(ChangeStreamDocument<Document> changeStreamDocument);

    default void handle(MongoCollection<Document> targetCollection,
                        ChangeStreamDocument<Document> changeStreamDocument) {

        switch (changeStreamDocument.getOperationType()) {
            case INSERT -> write(targetCollection, transform(changeStreamDocument));
            case DELETE -> delete(targetCollection, changeStreamDocument.getDocumentKey().get("_id"));
            default -> throw new IllegalArgumentException(changeStreamDocument.getOperationType() + " not supported");
        }

    }

    default BulkWriteResult write(MongoCollection<Document> targetCollection, List<WriteModel<Document>> writeModels) {
        return targetCollection.bulkWrite(writeModels);
    }

    default UpdateResult write(MongoCollection<Document> targetCollection, Document doc) {
        return targetCollection.replaceOne(
                Filters.eq("_id", doc.get("_id")),
                doc,
                new ReplaceOptions().upsert(true)
        );
    }

    default DeleteResult delete(MongoCollection<Document> targetCollection, Object docKey) {
        return targetCollection.deleteOne(Filters.eq("_id", docKey));
    }

}
