import com.scalien.scaliendb.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class Test {
    public static void main(String[] args) {
        try {
            final String databaseName = "testdb";
            final String tableName = "testtable";
            String[] nodes = {"127.0.0.1:7080"};

            Client.setTrace(true);
            
            Client client = new Client(nodes);
            Database db = new Database(client, databaseName);
            Table table = new Table(client, db, tableName);
            
            table.set("a", "0");
            table.add("a", 10);
            String value = table.get("a");

            System.out.println(value);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}