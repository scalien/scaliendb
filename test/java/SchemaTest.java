import com.scalien.scaliendb.*;

public class SchemaTest {
	
	public static void truncateTable(Client client) throws SDBPException {
		Database database = client.getDatabase("testdb");
		Table table = database.getTable("testtable");

		table.truncateTable();
	}
	
	public static void renameTable(Client client) throws SDBPException {
		Database database = client.getDatabase("testdb");
		Table table = database.getTable("testtable");

		table.renameTable("newName");
		table.renameTable("testtable");
	}
	
	public static void main(String[] args) {
        try {
            final String[] controllers = {"127.0.0.1:7080"};

            Client client = new Client(controllers);
            //client.setTrace(true);

			truncateTable(client);
            
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
		
	}
}
