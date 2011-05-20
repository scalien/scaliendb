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
            client.SetMasterTimeout(3 * 1000);
            client.SetGlobalTimeout(10 * 1000);
            client.UseDatabase("testdb");
            client.UseTable("testtable");
            client.Set("key", "value");
            string value = client.Get("key");
        }
    }
}
