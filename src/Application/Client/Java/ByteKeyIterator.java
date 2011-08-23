package com.scalien.scaliendb;

import java.util.List;
import java.util.Collections;

public class ByteKeyIterator implements java.lang.Iterable<byte[]>, java.util.Iterator<byte[]>
{
    private Table table;
    private byte[] startKey;
    private byte[] endKey;
    private byte[] prefix;
    private int count;
    private int gran = 100;
    private List<byte[]> keys;
    private int pos;
 
    public ByteKeyIterator(Table table, ByteRangeParams ps) throws SDBPException {
        this.table = table;
        this.startKey = ps.startKey;
        this.endKey = ps.endKey;
        this.prefix = ps.prefix;
        this.count = ps.count;
        
        query(false);
    }

    // for Iterable
    public java.util.Iterator<byte[]> iterator() {
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
    public byte[] next() {
        byte[] e = keys.get(pos);
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

        num = gran;
        if (count > 0 && count < gran)
            num = count;

        keys = table.getClient().listKeys(table.getTableID(), startKey, endKey, prefix, num, skip);
        Collections.sort(keys, new ByteArrayComparator());
        pos = 0;
    }
}