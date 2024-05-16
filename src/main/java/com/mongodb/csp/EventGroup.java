package com.mongodb.csp;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

import java.util.List;

public class EventGroup {
    private Long timeout;
    private List<ChangeStreamDocument<Document>> events;

    public EventGroup(Long timeout, List<ChangeStreamDocument<Document>> events) {
        this.timeout = timeout;
        this.events = events;
    }

    public Long getExpiry() {
        return timeout;
    }

    public List<ChangeStreamDocument<Document>> getEvents() {
        return events;
    }
}
