import com.scalien.scaliendb.Client;
import com.scalien.scaliendb.Result;
import com.scalien.scaliendb.Database;
import com.scalien.scaliendb.Table;
import com.scalien.scaliendb.SDBPException;
import java.util.Random;

public class TestDriver {

    public static void main (String [] args) throws SDBPException {
        Client client = new Client (new String[] {"127.0.0.1:7080"});
        client.setTrace(true);
        if (args [0].equals ("init")) {
            System.out.println("Initializing");
            long quorumId = client.createQuorum(new long[] {100});
            long databaseId = client.createDatabase("testdb");
            client.createTable(databaseId, quorumId, "testtable");
        } else if (args [0].equals ("fillrnd")) {
            Database database = new Database(client, "testdb");
            Table table = new Table(client, database, "testtable");
            Random random = new Random ();
            long start = System.currentTimeMillis();
            client.begin();
            int num = 100*1000*1000;
            for (int index = 0; index < num; index++) {
                if (index % 10000 == 0) {
                    if (index % 10000 == 0 && index != 0) {
                        client.submit();
                        client.begin();
                    }
                    System.out.println("Processing at " + index);
                }
                byte [] key = new byte [10];
                random.nextBytes (key);
                int size = random.nextInt(100) + 10;
                byte [] value = new byte [size];
                random.nextBytes (value);
                table.set(key, value);
            }
            client.submit();
            long end = System.currentTimeMillis();
            System.out.println("Elapsed: " + (end - start)/1000 + ", num: " + num + ", num/s: " + num/(end - start) * 1000.0);
        } else if (args [0].equals ("fill")) {
            Database database = new Database(client, "testdb");
            Table table = new Table(client, database, "testtable");
            Random random = new Random ();
            long start = System.currentTimeMillis();
            client.begin();
            int num = 1*1000*1000;
            for (int index = 0; index < num; index++) {
                if (index % 10000 == 0) {
                    if (index % 10000 == 0 && index != 0) {
                        client.submit();
                        client.begin();
                    }
                    System.out.println("Processing at " + index);
                }
                String key = "" + index;
                int size = random.nextInt(100) + 10;
                byte [] value = new byte [size];
                random.nextBytes (value);
                table.set(key.getBytes(), value);
            }
            client.submit();
            long end = System.currentTimeMillis();
            System.out.println("Elapsed: " + (end - start)/1000 + ", num: " + num + ", num/s: " + num/(end - start) * 1000.0);
        } else if (args [0].equals ("query")) {
            Database database = new Database(client, "testdb");
            Table table = new Table(client, database, "testtable");
            Random random = new Random ();
            client.begin();
            for (int index = 0; index < 1000; index++) {
                if (index % 100 == 0)
                    System.out.println("Processing at " + index);
                byte [] key = new byte [10];
                random.nextBytes (key);
                //table.get(key, null);
                table.get("" + index);
            }
            client.submit();
            Result result = client.getResult();
            for (result.begin(); !result.isEnd(); result.next())
            {
                  if (result.getCommandStatus() == 0)
                      System.out.println("value = " + result.getValue());
                  else
                      System.out.println("Command status: " + result.getCommandStatus());
            }
        }
    }
}