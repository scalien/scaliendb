import com.scalien.scaliendb.*;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Random;

public class SubtreeMatch {
   	public static void main(String[] args) {
		try {
			String[] controllers = {"127.0.0.1:7080"};
			boolean createSchema = false;

			if (args.length > 0) {
			    controllers[0] = args[0];
			    if (args.length > 1) {
			        if (args[1].equals("createSchema"))
			            createSchema = true;
			    }
			}

			System.out.println("Creating random page...");
			Random random = new Random(System.nanoTime());
			ArrayList<String> randomKeys = new ArrayList<String>();
			MessageDigest md = MessageDigest.getInstance("MD5");
			for (int i = 0; i < 1000; i++)
			{
				long docID = random.nextInt(100000);
				String rnds = "" + docID;
				byte[] value = md.digest(rnds.getBytes("UTF-8"));
				randomKeys.add(value.toString());
			}

			System.out.println("Querying random digests from database...");
			Client client = new Client(controllers);
			//client.setTrace(true);

			if (createSchema) {
			    long[] nodes = {100};
			    long quorumID = client.createQuorum(nodes);
			    long databaseID = client.createDatabase("test");
			    long tableID = client.createTable(databaseID, quorumID, "subtree");
			}

			client.useDatabase("test");
			client.useTable("subtree");

			String value;
			value = "TODO";

			client.begin();
			for (String key : randomKeys)
				client.get(key, value);
			
			System.out.println("Submitting 'get's to database...");
			long startTime = System.currentTimeMillis();
			client.submit();
			long endTime = System.currentTimeMillis();
			
			long getps = (long)(randomKeys.size() / (float)(endTime - startTime) * 1000.0 + 0.5);
			System.out.println("get/s: " + getps);
			
 			ArrayList<String> insertKeys = new ArrayList<String>();
   			Result result = client.getResult();
			for (result.begin(); !result.isEnd(); result.next()) {
				if (result.getCommandStatus() == Status.SDBP_FAILED) {
					insertKeys.add(result.getKey());
				}
			}

			client.begin();
			for (String key : insertKeys)
				client.getAndSet(key, value);
			
			System.out.println("Submitting 'set's to database...");
			startTime = System.currentTimeMillis();
			client.submit();
			endTime = System.currentTimeMillis();
			
			long setps = (long)(randomKeys.size() / (float)(endTime - startTime) * 1000.0 + 0.5);
			System.out.println("Total sets: " + insertKeys.size() + ", set/s: " + setps);
			
		} catch (Exception e) {
			System.err.println("Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}
}