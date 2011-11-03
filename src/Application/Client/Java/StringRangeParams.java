package com.scalien.scaliendb;

/**
 * StringRangeParams is a convenient way to specify the string parameters
 * for iteration when using
 * <a href="Table.html#getKeyIterator(com.scalien.scaliendb.StringRangeParams)">Table.getKeyIterator(StringRangeParams)</a>,
 * <a href="Table.html#getKeyValueIterator(com.scalien.scaliendb.StringRangeParams)">Table.getKeyValueIterator(StringRangeParams)</a> and
 * <a href="Table.html#count(com.scalien.scaliendb.StringRangeParams)">Table.count(StringRangeParams)</a>.
 * <p>
 * The supported parameters are:
 * <ul>
 * <li>prefix</li>
 * <li>start key</li>
 * <li>end key</li>
 * <li>count</li>
 * <li>direction</li>
 * </ul>
 * <p>
 * StringRangeParams is convenient because is uses chaining, so you can write
 * expressions like <code>new StringRangeParams().prefix(prefix).startKey(startKey).endKey(endKey).count(count)</code>
 * and it returns the StringRangeParams instance. Optionally call .backward() to get a backward iterator.
 * <p>
 * The default values are empty strings and forward iteration.
 * <p>
 * Example:
 * <pre>
 * // print keys that start with "foo", starting at "foobar"
 * for (String key : table.getKeyIterator(new StringRangeParams().prefix("foo").startKey("foobar")))
 *     System.out.println(key);
 * </pre>
 * @see Table#getKeyIterator(StringRangeParams) 
 * @see Table#getKeyValueIterator(StringRangeParams) 
 * @see Table#count(StringRangeParams) 
 */
public class StringRangeParams
{
    public String prefix = "";
    public String startKey = "";
    public String endKey = "";
    public int count = -1;
    public boolean forwardDirection = true;
    
    ByteRangeParams toByteRangeParams()
    {
        ByteRangeParams ps = new ByteRangeParams();

        ps.prefix = Client.stringToByteArray(prefix);
        ps.startKey = Client.stringToByteArray(startKey);
        ps.endKey = Client.stringToByteArray(endKey);
        ps.count = count;
        ps.forwardDirection = forwardDirection;
            
        return ps;
    }

    /**
     * Specify the prefix parameter for iteration.
     * <p>Only keys starting with prefix will be returned by the iteration.
     * @param prefix The prefix parameter as a string.
     * @return The StringRangeParams instance, useful for chaining.
     */
    public StringRangeParams prefix(String prefix)
    {
        this.prefix = prefix;
        return this;
    }

    /**
     * Specify the start key parameter for iteration.
     * <p>Iteration will start at start key, or the first key greater than start key.
     * @param startKey The start key parameter as a string.
     * @return The StringRangeParams instance, useful for chaining.
     */    
    public StringRangeParams startKey(String startKey)
    {
        this.startKey = startKey;
        return this;
    }

    /**
     * Specify the end key parameter for iteration
     * <p>Iteration will stop at end key, or the first key greater than end key.
     * @param endKey The end key parameter as a string.
     * @return The StringRangeParams instance, useful for chaining.
     */    
    public StringRangeParams endKey(String endKey)
    {
        this.endKey = endKey;
        return this;
    }

    /**
     * Specify the count parameter for iteration
     * <p>Iteration will stop after count elements.
     * @param count The count parameter.
     * @return The StringRangeParams instance, useful for chaining.
     */    
    public StringRangeParams count(int count)
    {
        this.count = count;
        return this;
    }

    /**
     * Iteration will proceed backwards
     * @return The StringRangeParams instance, useful for chaining.
     */    
    public StringRangeParams backward()
    {
        this.forwardDirection = false;
        return this;
    }
}
