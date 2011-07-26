package com.scalien.scaliendb;

public class StringIterParams
{
    public String prefix = "";
    public String startKey = "";
    public String endKey = "";
    
    public StringIterParams prefix(String prefix)
    {
        this.prefix = prefix;
        return this;
    }
    
    public StringIterParams startKey(String startKey)
    {
        this.startKey = startKey;
        return this;
    }
    
    public StringIterParams endKey(String endKey)
    {
        this.endKey = endKey;
        return this;
    }
}