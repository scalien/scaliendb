namespace Scalien
{
    /// <summary>
    /// StringIterParams is a convenient way to specify the string parameters
    /// for iteration when using
    /// <see cref="Table.KeyIterator(StringIterParams)"/>,
    /// <see cref="Table.KeyValueIterator(StringIterParams)"/> and
    /// <see cref="Table.Count(StringIterParams)"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The supported parameters are:
    /// <list type="bullet">
    /// <item>prefix</item>
    /// <item>start key</item>
    /// <item>end key</item>
    /// </list>
    /// </para>
    /// <para>
    /// StringIterParams is convenient because is uses chaining, so you can write
    /// expressions like <c>new StringIterParams().Prefix(prefix).StartKey(startKey).EndKey(endKey)</c>
    /// and it returns the StringIterParam instance.
    /// </para>
    /// <para>
    /// The default values are empty strings.
    /// </para>
    /// </remarks>
    /// <example><code>
    /// // print keys that start with "foo", starting at "foobar"
    /// foreach (string key in client.KeyIterator(new StringIterParams().Prefix("foo").StartKey("foobar")))
    ///     System.Console.WriteLine(key);
    /// </code></example>
    /// <seealso cref="Table.KeyIterator(StringIterParams)"/>
    /// <seealso cref="Table.KeyValueIterator(StringIterParams)"/>
    /// <seealso cref="Table.Count(StringIterParams)"/>
    public class StringIterParams
    {
        internal string prefix = "";
        internal string startKey = "";
        internal string endKey = "";

        /// <summary>Specify the prefix parameter for iteration.</summary>
        /// <remarks>Only keys starting with prefix will be returned by the iteration.</remarks>
        /// <param name="prefix">The prefix parameter as a string.</param>
        /// <returns>The StringIterParams instance, useful for chaining.</returns>
        public StringIterParams Prefix(string prefix)
        {
            this.prefix = prefix;
            return this;
        }

        /// <summary>Specify the start key parameter for iteration.</summary>
        /// <remarks>Iteration will start at start key, or the first key greater than start key.</remarks>
        /// <param name="startKey">The start key parameter as a string.</param>
        /// <returns>The StringIterParams instance, useful for chaining.</returns>
        public StringIterParams StartKey(string startKey)
        {
            this.startKey = startKey;
            return this;
        }

        /// <summary>Specify the end key parameter for iteration</summary>
        /// <remarks>Iteration will stop at end key, or the first key greater than end key.</remarks>
        /// <param name="endKey">The end key parameter as a string.</param>
        /// <returns>The StringIterParams instance, useful for chaining.</returns>
        public StringIterParams EndKey(string endKey)
        {
            this.endKey = endKey;
            return this;
        }
    }
}
