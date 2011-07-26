package com.scalien.scaliendb;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;

public class StringKeyValueIterator implements java.lang.Iterable<KeyValue<String,String>>, java.util.Iterator<KeyValue<String,String>>
{
    private Client client;
    private Table table;
    private String startKey;
    private String endKey;
    private String prefix;
    private int count;
    private LinkedList<String> keys;
    private LinkedList<String> values;
    private int pos;
 
    public StringKeyValueIterator(Client client, StringIterParams ps) throws SDBPException {
        this.client = client;
        this.startKey = ps.startKey;
        this.endKey = ps.endKey;
        this.prefix = ps.prefix;
        this.count = 100;
        
        query(false);
    }
    
    public StringKeyValueIterator(Table table, StringIterParams ps) throws SDBPException {
        this.table = table;
        this.startKey = ps.startKey;
        this.endKey = ps.endKey;
        this.prefix = ps.prefix;
        this.count = 100;
        
        query(false);
    }

    // for Iterable
    public java.util.Iterator<KeyValue<String,String>> iterator() {
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
    public KeyValue<String,String> next() {
        KeyValue<String, String> e = new KeyValue<String, String>(keys.get(pos), values.get(pos));
        pos++;
        return e;
    }
    
    // for Iterator
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
    
    private void query(boolean skip) throws SDBPException {
        Map<String, String> result;
        if (client != null)
            result = client.listKeyValues(startKey, endKey, prefix, count, skip);
        else
            result = table.listKeyValues(startKey, endKey, prefix, count, skip);
        keys = new LinkedList<String>();
        values = new LinkedList<String>();

        for (Map.Entry<String, String> entry : result.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            keys.add(key);
            values.add(value);
        }

        pos = 0;
    }    
}