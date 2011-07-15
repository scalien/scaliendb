package com.scalien.scaliendb;

import java.util.List;
import java.util.Collections;

public class StringKeyIterator implements java.lang.Iterable<String>, java.util.Iterator<String>
{
    private Client client;
    private Table table;
    private String startKey;
    private String endKey;
    private String prefix;
    private int count;
    private List<String> keys;
    private int pos;
 
    public StringKeyIterator(Client client, String startKey, String endKey, String prefix) throws SDBPException {
        this.client = client;
        this.startKey = startKey;
        this.endKey = endKey;
        this.prefix = prefix;
        this.count = 100;
        this.pos = 0;
        
        query(false);
    }
    
    public StringKeyIterator(Table table, String startKey, String endKey, String prefix) throws SDBPException {
        this.table = table;
        this.startKey = startKey;
        this.endKey = endKey;
        this.prefix = prefix;
        this.count = 100;
        this.pos = 0;
        
        query(false);
    }

    // for Iterable
    public java.util.Iterator<String> iterator() {
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
    public String next() {
        String e = keys.get(pos);
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
        Collections.sort(keys);
        pos = 0;
    }    
}