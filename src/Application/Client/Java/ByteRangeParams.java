package com.scalien.scaliendb;

/**
 * ByteRangeParams is a convenient way to specify the byte[] parameters
 * for iteration when using
 * <a href="Table.html#getKeyIterator(com.scalien.scaliendb.ByteRangeParams)">Table.getKeyIterator(ByteRangeParams)</a>,
 * <a href="Table.html#getKeyValueIterator(com.scalien.scaliendb.ByteRangeParams)">Table.getKeyValueIterator(ByteRangeParams)</a> and 
 * <a href="Table.html#count(com.scalien.scaliendb.ByteRangeParams)">Table.count(ByteRangeParams)</a>.
 * <p>
 * The supported parameters are:
 * <ul>
 * <li>prefix</li>
 * <li>start key</li>
 * <li>end key</li>
 * <li>count</li>
 * </ul>
 * <p>
 * ByteRangeParams is convenient because is uses chaining, so you can write
 * expressions like <code>new ByteRangeParams().prefix(prefix).startKey(startKey).endKey(endKey).count(count)</code>
 * and it returns the ByteIterParam instance.
 * <p>
 * The default values are empty byte arrays.
 * <p>
 * @see Table#getKeyIterator(ByteRangeParams) 
 * @see Table#getKeyValueIterator(ByteRangeParams) 
 * @see Table#count(ByteRangeParams) 
 */public class ByteRangeParams
{
    public byte[] prefix = new byte[0];
    public byte[] startKey = new byte[0];
    public byte[] endKey = new byte[0];
    public int count = -1;

    /**
     * Specify the prefix parameter for iteration.
     * <p>Only keys starting with prefix will be returned by the iteration.
     * @param prefix The prefix parameter as a byte[].
     * @return The ByteRangeParams instance, useful for chaining.
     */    
    public ByteRangeParams prefix(byte[] prefix)
    {
        this.prefix = prefix;
        return this;
    }
    
    /**
     * Specify the start key parameter for iteration.
     * <p>Iteration will start at start key, or the first key greater than start key.
     * @param startKey The start key parameter as a byte[].
     * @return The ByteRangeParams instance, useful for chaining.
     */
    public ByteRangeParams startKey(byte[] startKey)
    {
        this.startKey = startKey;
        return this;
    }
    
    /**
     * Specify the end key parameter for iteration
     * <p>Iteration will stop at end key, or the first key greater than end key.
     * @param endKey The end key parameter as a byte[].
     * @return The ByteRangeParams instance, useful for chaining.
     */
    public ByteRangeParams endKey(byte[] endKey)
    {
        this.endKey = endKey;
        return this;
    }

    /**
     * Specify the count parameter for iteration
     * <p>Iteration will stop after count elements.
     * @param count The count parameter.
     * @return The ByteRangeParams instance, useful for chaining.
     */    
    public ByteRangeParams count(int count)
    {
        this.count = count;
        return this;
    }
}