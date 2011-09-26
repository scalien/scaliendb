import com.scalien.scaliendb.*;

public class SequenceTest {
	
	public static void getNextSequenceNumber(Client client) throws SDBPException {
		Database database = client.getDatabase("test");

		database.setSequenceGranularity("test", "test", 1000);
		database.resetSequenceNumber("test", "test");

		for (int i = 0; i < 1000000; i++) {
			long s = database.getSequenceNumber("test", "test");
			System.out.println(s);
		}
	}
	
	public static void main(String[] args) {
        try {
            final String[] controllers = {"192.168.137.100:7080", "192.168.137.101:7080", "192.168.137.102:7080"};

            Client client = new Client(controllers);
            //client.setTrace(true);

			getNextSequenceNumber(client);
            
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
		
	}
}
