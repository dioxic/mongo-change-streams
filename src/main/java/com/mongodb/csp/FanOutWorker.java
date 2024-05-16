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

public class FanOutWorker implements Callable<Integer> {

    private final Logger logger = LoggerFactory.getLogger(FanOutWorker.class);
    private final BlockingQueue<ChangeStreamDocument<Document>> inQueue;
    private final List<BlockingQueue<ChangeStreamDocument<Document>>> outQueues;

    public FanOutWorker(BlockingQueue<ChangeStreamDocument<Document>> inQueue,
                        List<BlockingQueue<ChangeStreamDocument<Document>>> outQueues) {
        this.inQueue = inQueue;
        this.outQueues = outQueues;
    }

    @Override
    public Integer call() throws Exception {
//        for (ChangeStreamDocument<Document> e : inQueue) {
//            try {
//                e.getFullDocument()
//            } catch (InterruptedException ex) {
//                logger.error(ex.getMessage(), ex);
//                return 1;
//            }
//        }
        return 0;
    }
}
