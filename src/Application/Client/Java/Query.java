package com.scalien.scaliendb;

public class Query
{
    private Table table;
    private byte[] prefix;
    private byte[] startKey;
    private byte[] endKey;
    private int count;
    
    Query(Table table) {
        this.table = table;
        prefix = new byte[0];
        startKey = new byte[0];
        endKey = new byte[0];
        count = 0;
    }
    
    public void setPrefix(String prefix) {
        this.prefix = prefix.getBytes();
    }
    
    public void setPrefix(byte[] prefix) {
        this.prefix = prefix;
    }
    
    public void setStartKey(String startKey) {
        this.startKey = startKey.getBytes();
    }
    
    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }
    
    public void setEndKey(String endKey) {
        this.endKey = endKey.getBytes();
    }
    
    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }
    
    public void setCount(int count) {
        this.count = count;
    }
    
    public ByteKeyIterator getByteKeyIterator() throws SDBPException {
        ByteRangeParams params = new ByteRangeParams();

        params.prefix(prefix);
        params.startKey(startKey);
        params.endKey(endKey);
        params.count(count);

        return new ByteKeyIterator(table, params);
    }
    
    public ByteKeyValueIterator getByteKeyValueIterator() throws SDBPException {
        ByteRangeParams params = new ByteRangeParams();

        params.prefix(prefix);
        params.startKey(startKey);
        params.endKey(endKey);
        params.count(count);
        
        return new ByteKeyValueIterator(table, params);
    }
    
    public StringKeyIterator getStringKeyIterator() throws SDBPException {
        StringRangeParams params = new StringRangeParams();

        params.prefix(new String(prefix));
        params.startKey(new String(startKey));
        params.endKey(new String(endKey));
        params.count(count);

        return new StringKeyIterator(table, params);
    }

    public StringKeyValueIterator getStringKeyValueIterator() throws SDBPException {
        StringRangeParams params = new StringRangeParams();

        params.prefix(new String(prefix));
        params.startKey(new String(startKey));
        params.endKey(new String(endKey));
        params.count(count);

        return new StringKeyValueIterator(table, params);
    }
}