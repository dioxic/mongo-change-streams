package com.mongodb.csp.model;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;


public record EventTransformPair<T>(ChangeStreamDocument<Document> event, T transformedDoc){}