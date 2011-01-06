import com.scalien.scaliendb.Client;
import com.scalien.scaliendb.Result;
import com.scalien.scaliendb.Database;
import com.scalien.scaliendb.Table;
import com.scalien.scaliendb.SDBPException;
import java.util.Random;

class ClientThread extends Thread {
	public void run() {
		try {
			Client client = new Client(new String[] {"127.0.0.1:7080"});
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
		} catch (SDBPException e) {
			System.out.println(e.toString());
		}
	}
}

public class MultiThreadTest {
    public static void main (String [] args) {
		for (int i = 0; i < 10; i++)
		{
			ClientThread ct = new ClientThread();
			ct.start();
		}
    }
}