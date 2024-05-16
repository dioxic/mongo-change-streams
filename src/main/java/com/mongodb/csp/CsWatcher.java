package com.mongodb.csp;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

public class CsWatcher implements Callable<Integer> {

    private final Logger logger = LoggerFactory.getLogger(CsWatcher.class);
    private final List<Bson> pipeline;
    private final MongoClient client;
    private final BlockingQueue<ChangeStreamDocument<Document>> queue;

    public CsWatcher(List<Bson> pipeline, MongoClient client, BlockingQueue<ChangeStreamDocument<Document>> queue) {
        this.pipeline = pipeline;
        this.client = client;
        this.queue = queue;
    }

    @Override
    public Integer call() throws Exception {
        for (ChangeStreamDocument<Document> e : client.watch(pipeline)) {
            try {
                queue.put(e);
            } catch (InterruptedException ex) {
                logger.error(ex.getMessage(), ex);
                return 1;
            }
        }
        return 0;
    }
}
