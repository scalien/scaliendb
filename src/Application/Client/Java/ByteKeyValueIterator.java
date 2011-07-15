package com.scalien.scaliendb;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;

public class ByteKeyValueIterator implements java.lang.Iterable<KeyValue<byte[], byte[]>>, java.util.Iterator<KeyValue<byte[], byte[]>>
{
    private Client client;
    private Table table;
    private byte[] startKey;
    private byte[] endKey;
    private byte[] prefix;

    private int count;
    private int pos;
    private LinkedList<byte[]> keys;
    private LinkedList<byte[]> values;
 
    public ByteKeyValueIterator(Client client, byte[] startKey, byte[] endKey, byte[] prefix) throws SDBPException {
        this.client = client;
        this.startKey = startKey;
        this.endKey = endKey;
        this.prefix = prefix;
        this.count = 100;
        this.pos = 0;
        
        query(false);
    }
    
    public ByteKeyValueIterator(Table table, byte[] startKey, byte[] endKey, byte[] prefix) throws SDBPException {
        this.table = table;
        this.startKey = startKey;
        this.endKey = endKey;
        this.prefix = prefix;
        this.count = 100;
        this.pos = 0;
        
        query(false);
    }

    // for Iterable
    public java.util.Iterator<KeyValue<byte[], byte[]>> iterator() {
        return this;
    }
    
    // for Iterator
    public boolean hasNext() {
        try {
            if (pos == keys.size()) {
                if (keys.size() < count)
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
        return e;
    }
    
    // for Iterator
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
    
    private void query(boolean skip) throws SDBPException {
        Map<byte[], byte[]> result;
        if (client != null)
            result = client.listKeyValues(startKey, endKey, prefix, count, skip);
        else
            result = table.listKeyValues(startKey, endKey, prefix, count, skip);
        keys = new LinkedList<byte[]>();
        values = new LinkedList<byte[]>();
        TreeSet<byte[]> ts = new TreeSet<byte[]>(result.keySet());
        for (byte[] key : ts) { 
           byte[] value = result.get(key);
           keys.add(key);
           values.add(value);
        }
        pos = 0;
    }    
}