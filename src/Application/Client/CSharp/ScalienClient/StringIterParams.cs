namespace Scalien
{
    public class StringIterParams
    {
        public string prefix = "";
        public string startKey = "";
        public string endKey = "";

        public StringIterParams Prefix(string prefix)
        {
            this.prefix = prefix;
            return this;
        }

        public StringIterParams StartKey(string startKey)
        {
            this.startKey = startKey;
            return this;
        }

        public StringIterParams EndKey(string endKey)
        {
            this.endKey = endKey;
            return this;
        }
    }
}
