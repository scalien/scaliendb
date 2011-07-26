package com.scalien.scaliendb;

public class StringRangeParams
{
    public String prefix = "";
    public String startKey = "";
    public String endKey = "";
    
    public StringRangeParams prefix(String prefix)
    {
        this.prefix = prefix;
        return this;
    }
    
    public StringRangeParams startKey(String startKey)
    {
        this.startKey = startKey;
        return this;
    }
    
    public StringRangeParams endKey(String endKey)
    {
        this.endKey = endKey;
        return this;
    }
}
