namespace Scalien
{
    public class ByteIterParams
    {
        public byte[] prefix = new byte[0];
        public byte[] startKey = new byte[0];
        public byte[] endKey = new byte[0];

        public ByteIterParams Prefix(byte[] prefix)
        {
            this.prefix = prefix;
            return this;
        }

        public ByteIterParams StartKey(byte[] startKey)
        {
            this.startKey = startKey;
            return this;
        }

        public ByteIterParams EndKey(byte[] endKey)
        {
            this.endKey = endKey;
            return this;
        }
    }
}