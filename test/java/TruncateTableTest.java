import com.scalien.scaliendb.*;

public class TruncateTableTest {
    public static void main(String[] args) {
        try {
            final String[] controllers = {"127.0.0.1:7080"};

            Client client = new Client(controllers);
            client.setTrace(true);

			Database database = client.getDatabase("testdb");
			Table table = database.getTable("testtable");

			table.truncateTable();
            
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}