package com.scalien.scaliendb;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

public class ByteKeyValueIterator implements java.lang.Iterable<KeyValue<byte[], byte[]>>, java.util.Iterator<KeyValue<byte[], byte[]>>
{
    private Table table;
    private byte[] startKey;
    private byte[] endKey;
    private byte[] prefix;
    private int count;
    private boolean forwardDirection;
    private int gran = 100;
    private int pos;
    private LinkedList<byte[]> keys;
    private LinkedList<byte[]> values;
 
    public ByteKeyValueIterator(Table table, ByteRangeParams ps) throws SDBPException {
        this.table = table;
        this.startKey = ps.startKey;
        this.endKey = ps.endKey;
        this.prefix = ps.prefix;
        this.count = ps.count;
        this.forwardDirection = ps.forwardDirection;
        
        query(false);
    }

    // for Iterable
    public java.util.Iterator<KeyValue<byte[], byte[]>> iterator() {
        return this;
    }
    
    // for Iterator
    public boolean hasNext() {
        try {
            if (count == 0)
                return false;
            if (pos == keys.size()) {
                if (keys.size() < gran)
                    return false;
                startKey = keys.get(keys.size()-1);
                query(true);
            }
            if (keys.size() == 0)
                return false;
            return true;
        }
        catch(SDBPException e) {
            return false;
        }
    }
    
    // for Iterator
    public KeyValue<byte[], byte[]> next() {
        KeyValue<byte[], byte[]> e = new KeyValue<byte[], byte[]>(keys.get(pos), values.get(pos));
        pos++;
        if (count > 0)
            count--;
        return e;
    }
    
    // for Iterator
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
    
    private void query(boolean skip) throws SDBPException {
        int num;
        Map<byte[], byte[]> result;

        num = gran;
        if (count > 0 && count < gran)
            num = count;

        result = table.getClient().listKeyValues(
         table.getTableID(), startKey, endKey, prefix, num, forwardDirection, skip);
        keys = new LinkedList<byte[]>();
        values = new LinkedList<byte[]>();

        for (Map.Entry<byte[], byte[]> entry : result.entrySet()) {
            byte[] key = entry.getKey();
            byte[] value = entry.getValue();
            keys.add(key);
            values.add(value);
        }

        pos = 0;
    }    
}