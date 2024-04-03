package com.mongodb.csp.model;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

import java.util.List;

public class EventBatchException extends Throwable {

    private List<ChangeStreamDocument<Document>> events;

    private MongoBulkWriteException mongoException;

    public EventBatchException(List<ChangeStreamDocument<Document>> events, MongoBulkWriteException mongoException) {
        this.events = events;
        this.mongoException = mongoException;
    }

    public List<ChangeStreamDocument<Document>> getEvents() {
        return events;
    }

    public MongoBulkWriteException getMongoException() {
        return mongoException;
    }

    public List<Document> getErrorDocuments() {
        return mongoException.getWriteErrors().stream()
                .map(ex -> {
                    var doc = new Document();
                    doc.put("errMessage", ex.getMessage());
                    doc.put("event", events.get(ex.getIndex()));
                    return doc;
                })
                .toList();
    }
}
