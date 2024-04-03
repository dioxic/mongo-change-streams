package com.mongodb.csp.model;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.List;

public record EventBatchResult<T>(List<ChangeStreamDocument<Document>> events, T result) {

    private static final EventComparator eventComparator = new EventComparator();

    BsonDocument getLatestResumeToken() {
//        events.sort(eventComparator);
        var last = events.get(events.size()-1);
        return last.getResumeToken();
    }
}