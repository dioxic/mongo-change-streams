package com.mongodb.csp.processors;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.in;

public class AuditingProcessor implements Processor {

    @Override
    public List<Bson> getChangeStreamPipeline() {
        return List.of(
                match(and(
                        in("operationType", List.of("insert", "update", "delete"))
                ))
        );
    }

    @Override
    public WriteModel<Document> transform(ChangeStreamDocument<Document> changeStreamDocument) {
        var doc = new Document();

        doc.put("docKey", changeStreamDocument.getDocumentKey());
        doc.put("clusterTime", changeStreamDocument.getClusterTime());
        doc.put("fullDocument", changeStreamDocument.getFullDocument());
        doc.put("operationType", changeStreamDocument.getOperationType());
        doc.put("resumeToken", changeStreamDocument.getResumeToken());

        return new InsertOneModel<>(doc);
    }

}
