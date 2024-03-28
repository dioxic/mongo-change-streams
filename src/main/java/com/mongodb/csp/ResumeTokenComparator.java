package com.mongodb.csp;

import org.bson.BsonDocument;

import java.util.Comparator;

public class ResumeTokenComparator implements Comparator<BsonDocument> {
    @Override
    public int compare(BsonDocument o1, BsonDocument o2) {
        return compare(getBinary(o1), getBinary(o2));
    }

    private byte[] getBinary(BsonDocument doc) {
        return doc.getBinary("_data").getData();
    }

    public int compare(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }
}

