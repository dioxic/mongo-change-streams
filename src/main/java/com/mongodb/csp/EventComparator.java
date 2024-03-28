package com.mongodb.csp;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

import java.util.Comparator;

public class EventComparator implements Comparator<ChangeStreamDocument<Document>> {
    
    private static final ResumeTokenComparator resumeTokenComparator = new ResumeTokenComparator();

    @Override
    public int compare(ChangeStreamDocument<Document> o1, ChangeStreamDocument<Document> o2) {
        return resumeTokenComparator.compare(o1.getResumeToken(), o2.getResumeToken());
    }
}
