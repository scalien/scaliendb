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
 * </ul>
 * <p>
 * StringRangeParams is convenient because is uses chaining, so you can write
 * expressions like <code>new StringRangeParams().prefix(prefix).startKey(startKey).endKey(endKey)</code>
 * and it returns the StringRangeParams instance.
 * <p>
 * The default values are empty strings.
 * <p>
 * <p>
 * Example:
 * <pre>
 * // print keys that start with "foo", starting at "foobar"
 * for (String key : client.getKeyIterator(new StringRangeParams().prefix("foo").startKey("foobar")))
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
}
