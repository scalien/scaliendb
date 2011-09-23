import com.scalien.scaliendb.*;

public class QueryTest {
	
	public static void queryTest(Client client) throws SDBPException {
		Database database = client.getDatabase("test");
		Table table = database.getTable("test");
		Query query = table.createQuery();
		query.setCount(10);
		for (KeyValue<String, String> kv : query.getStringKeyValueIterator()) {
			System.out.println(kv.getKey() + " => " + kv.getValue());
		}
	}
	
	public static void main(String[] args) {
        try {
            final String[] controllers = {"192.168.137.100:7080", "192.168.137.101:7080", "192.168.137.102:7080"};

            Client client = new Client(controllers);
            // client.setTrace(true);

			queryTest(client);
            
        } catch (Exception e) {
            System.err.println("Exception: " + e.toString());
        }
		
	}
}
