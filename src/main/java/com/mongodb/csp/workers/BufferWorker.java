package com.mongodb.csp.workers;

import com.mongodb.csp.EventGroup;

import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Takes a concurrent map and continuously scans for entries that have expired.
 * <p>
 * Expired entires are added to the output queue and removed from the concurrent map.
 */
public class BufferWorker implements Callable<Integer> {

    private final ConcurrentHashMap<String, EventGroup> map;
    private final BlockingQueue<EventGroup> outputQueue;

    public BufferWorker(ConcurrentHashMap<String, EventGroup> map, BlockingQueue<EventGroup> outputQueue) {
        this.map = map;
        this.outputQueue = outputQueue;
    }

    @Override
    public Integer call() throws Exception {
        while (true) {
            map.forEach(10, (k, v) -> {
                if (v.getExpiry() > Instant.now().toEpochMilli()) {
                    map.compute(k, (k2, v2) -> {
                        if (v.getExpiry() > Instant.now().toEpochMilli()) {
                            try {
                                outputQueue.put(v);
                                return null; // atomically removing the entry from the map
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        return v2;
                    });
                }
            });
        }
    }
}
