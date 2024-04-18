package com.mongodb.csp.processors;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.in;

public class ExampleProcessor implements Processor {

    @Override
    public List<Bson> getChangeStreamPipeline() {
        return List.of(
                match(and(
                        in("operationType", List.of("insert", "delete"))
                ))
        );
    }

    @Override
    public Document transform(ChangeStreamDocument<Document> changeStreamDocument) {
        var doc = new Document();

        doc.put("_id", changeStreamDocument.getDocumentKey().get("_id"));
        doc.put("name", changeStreamDocument.getFullDocument().get("name"));

        var debugDoc = new Document();
        debugDoc.put("clusterTime", changeStreamDocument.getClusterTime());
        debugDoc.put("fullDocument", changeStreamDocument.getFullDocument());
        debugDoc.put("operationType", changeStreamDocument.getOperationType());
        debugDoc.put("resumeToken", changeStreamDocument.getResumeToken());

        if (changeStreamDocument.getExtraElements() != null) {
            debugDoc.put("workerId", changeStreamDocument.getExtraElements().get("workerId"));
        }

        doc.put("debug", debugDoc);

        return doc;
    }

}
