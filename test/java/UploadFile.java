import com.scalien.scaliendb.Client;
import com.scalien.scaliendb.Result;
import com.scalien.scaliendb.Database;
import com.scalien.scaliendb.Table;
import com.scalien.scaliendb.SDBPException;
import java.util.Random;
import java.io.FileInputStream;
import java.io.IOException;

public class UploadFile {

    public static void main (String [] args) throws SDBPException, IOException {

		Client client = new Client (new String [] {"127.0.0.1:7080"});
        Database db = client.getDatabase ("testdb");
        Table table = db.getTable ("testtable");
                                 
		String filename = args[0];
        String key = args[1];
        FileInputStream is = new FileInputStream (args[0]);
        byte[] data = new byte[(int) is.getChannel().size()];
        is.read(data);
        is.close();
        table.set(key.getBytes(), data);
	}
}
