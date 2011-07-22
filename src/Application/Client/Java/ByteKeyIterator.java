package com.scalien.scaliendb;

import java.util.List;
import java.util.Collections;

public class ByteKeyIterator implements java.lang.Iterable<byte[]>, java.util.Iterator<byte[]>
{
    private Client client;
    private Table table;
    private byte[] startKey;
    private byte[] endKey;
    private byte[] prefix;
    private int count;
    private List<byte[]> keys;
    private int pos;
 
    public ByteKeyIterator(Client client, byte[] startKey, byte[] endKey, byte[] prefix) throws SDBPException {
        this.client = client;
        this.startKey = startKey;
        this.endKey = endKey;
        this.prefix = prefix;
        this.count = 100;
        
        query(false);
    }
    
    public ByteKeyIterator(Table table, byte[] startKey, byte[] endKey, byte[] prefix) throws SDBPException {
        this.table = table;
        this.startKey = startKey;
        this.endKey = endKey;
        this.prefix = prefix;
        this.count = 100;
        
        query(false);
    }

    // for Iterable
    public java.util.Iterator<byte[]> iterator() {
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
    public byte[] next() {
        byte[] e = keys.get(pos);
        pos++;
        return e;
    }
    
    // for Iterator
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
    
    private void query(boolean skip) throws SDBPException {
        if (client != null)
            keys = client.listKeys(startKey, endKey, prefix, count, skip);
        else
            keys = table.listKeys(startKey, endKey, prefix, count, skip);
        Collections.sort(keys, new ByteArrayComparator());
        pos = 0;
    }
}