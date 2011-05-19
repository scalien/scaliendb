namespace Scalien
{
    public class Test
    {
        // test entry point
        public static void Main(string[] args)
        {
            
            Client.SetTrace(true);
            string[] nodes = { "192.168.137.236:7080" };
            Client client = new Client(nodes);
            client.UseDatabase("testdb");
            client.UseTable("testtable");
            client.Set("key", "value");
            string value = client.Get("key");
        }
    }
}
