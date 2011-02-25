import com.scalien.scaliendb.Client;
import com.scalien.scaliendb.Result;
import com.scalien.scaliendb.Database;
import com.scalien.scaliendb.Table;
import com.scalien.scaliendb.SDBPException;
import java.util.Random;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class CountTest {

    public static void main (String [] args) throws SDBPException, IOException {

		Client client = new Client (new String [] {"127.0.0.1:7080"});
        Database db = client.getDatabase ("testdb");
        Table table = db.getTable ("testtable");
		// client.setTrace(true);
        client.useDatabase("testdb");
		client.useTable("testtable");
		long num = client.count("", 0, 0);
		System.out.println(num);
	}
}
