package com.mongodb.csp.model;

import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;


public record EventModelPair<T>(ChangeStreamDocument<Document> event, WriteModel<T> model){}