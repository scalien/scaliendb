namespace Scalien
{
    /// <summary>
    /// ByteIterParams is a convenient way to specify the byte[] parameters
    /// for iteration when using
    /// <see cref="Table.KeyIterator(ByteIterParams)"/>,
    /// <see cref="Table.KeyValueIterator(ByteIterParams)"/>,
    /// <see cref="Table.Count(ByteIterParams)"/>.
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
    /// ByteIterParams is convenient because is uses chaining, so you can write
    /// expressions like <c>new ByteIterParams().Prefix(prefix).StartKey(startKey).EndKey(endKey)</c>
    /// and it returns the ByteIterParam instance.
    /// </para>
    /// <para>
    /// The default values are empty byte arrays.
    /// </para>
    /// </remarks>
    /// <seealso cref="Table.KeyIterator(ByteIterParams)"/>
    /// <seealso cref="Table.KeyValueIterator(ByteIterParams)"/>
    /// <seealso cref="Table.Count(ByteIterParams)"/>
    public class ByteIterParams
    {
        internal byte[] prefix = new byte[0];
        internal byte[] startKey = new byte[0];
        internal byte[] endKey = new byte[0];

        /// <summary>Specify the prefix parameter for iteration.</summary>
        /// <remarks>Only keys starting with prefix will be returned by the iteration.</remarks>
        /// <param name="prefix">The prefix parameter as a byte[].</param>
        /// <returns>The ByteIterParams instance, useful for chaining.</returns>
        public ByteIterParams Prefix(byte[] prefix)
        {
            this.prefix = prefix;
            return this;
        }

        /// <summary>Specify the start key parameter for iteration.</summary>
        /// <remarks>Iteration will start at start key, or the first key greater than start key.</remarks>
        /// <param name="startKey">The start key parameter as a byte[].</param>
        /// <returns>The ByteIterParams instance, useful for chaining.</returns>
        public ByteIterParams StartKey(byte[] startKey)
        {
            this.startKey = startKey;
            return this;
        }

        /// <summary>Specify the end key parameter for iteration</summary>
        /// <remarks>Iteration will stop at end key, or the first key greater than end key.</remarks>
        /// <param name="endKey">The end key parameter as a byte[].</param>
        /// <returns>The ByteIterParams instance, useful for chaining.</returns>
        public ByteIterParams EndKey(byte[] endKey)
        {
            this.endKey = endKey;
            return this;
        }
    }
}