package com.scalien.scaliendb;

public class ByteRangeParams
{
    public byte[] prefix = new byte[0];
    public byte[] startKey = new byte[0];
    public byte[] endKey = new byte[0];
    
    public ByteRangeParams prefix(byte[] prefix)
    {
        this.prefix = prefix;
        return this;
    }
    
    public ByteRangeParams startKey(byte[] startKey)
    {
        this.startKey = startKey;
        return this;
    }
    
    public ByteRangeParams endKey(byte[] endKey)
    {
        this.endKey = endKey;
        return this;
    }
}